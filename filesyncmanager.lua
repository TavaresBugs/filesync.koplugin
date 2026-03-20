local Device = require("device")
local InfoMessage = require("ui/widget/infomessage")
local NetworkMgr = require("ui/network/manager")
local UIManager = require("ui/uimanager")
local logger = require("logger")
local _ = require("gettext")
local T = require("ffi/util").template

local FileSyncManager = {
    _running = false,
    _server = nil,
    _port = nil,
    _ip = nil,
    _was_running_before_suspend = false,
}

local DEFAULT_PORT = 8080

function FileSyncManager:getPort()
    if self._port then return self._port end
    self._port = G_reader_settings:readSetting("filesync_port", DEFAULT_PORT)
    return self._port
end

function FileSyncManager:setPort(port)
    self._port = port
    G_reader_settings:saveSetting("filesync_port", port)
    G_reader_settings:flush()
end

function FileSyncManager:getSafeMode()
    return G_reader_settings:readSetting("filesync_safe_mode", true)
end

function FileSyncManager:setSafeMode(enabled)
    G_reader_settings:saveSetting("filesync_safe_mode", enabled)
    G_reader_settings:flush()
end

function FileSyncManager:configurePort()
    local InputDialog = require("ui/widget/inputdialog")
    local port_dialog
    port_dialog = InputDialog:new{
        title = _("Server port"),
        input = tostring(self:getPort()),
        input_type = "number",
        input_hint = "8080",
        buttons = {
            {
                {
                    text = _("Cancel"),
                    id = "close",
                    callback = function()
                        UIManager:close(port_dialog)
                    end,
                },
                {
                    text = _("Save"),
                    is_enter_default = true,
                    callback = function()
                        local new_port = tonumber(port_dialog:getInputText())
                        if new_port and new_port >= 1024 and new_port <= 65535 then
                            self:setPort(new_port)
                            UIManager:close(port_dialog)
                            UIManager:show(InfoMessage:new{
                                text = T(_("Port set to %1. Restart the server for changes to take effect."), new_port),
                                timeout = 3,
                            })
                        else
                            UIManager:show(InfoMessage:new{
                                text = _("Invalid port. Please enter a number between 1024 and 65535."),
                                timeout = 3,
                            })
                        end
                    end,
                },
            },
        },
    }
    UIManager:show(port_dialog)
    port_dialog:onShowKeyboard()
end

function FileSyncManager:getLocalIP()
    -- Try multiple methods to get the local IP address
    -- Method 1: Use KOReader's NetworkMgr if available
    if NetworkMgr and NetworkMgr.getLocalIpAddress then
        local ip = NetworkMgr:getLocalIpAddress()
        if ip and ip ~= "0.0.0.0" and ip ~= "127.0.0.1" then
            return ip
        end
    end

    -- Method 2: Parse ifconfig output
    local fd = io.popen("ifconfig 2>/dev/null || ip addr show 2>/dev/null")
    if fd then
        local output = fd:read("*all")
        fd:close()
        if output then
            -- Match inet addresses, skip loopback
            for ip in output:gmatch("inet%s+(%d+%.%d+%.%d+%.%d+)") do
                if ip ~= "127.0.0.1" then
                    return ip
                end
            end
        end
    end

    -- Method 3: UDP socket trick (doesn't actually send data)
    local socket = require("socket")
    local s = socket.udp()
    if s then
        s:setpeername("8.8.8.8", 80)
        local ip = s:getsockname()
        s:close()
        if ip and ip ~= "0.0.0.0" then
            return ip
        end
    end

    return nil
end

function FileSyncManager:getRootDir()
    -- Determine the books/library directory based on device
    if Device:isKindle() then
        return "/mnt/us"
    elseif Device:isKobo() then
        return "/mnt/onboard"
    elseif Device:isPocketBook() then
        return "/mnt/ext1"
    elseif Device:isAndroid() then
        return require("android").getExternalStoragePath()
    else
        -- Fallback: use KOReader's home directory
        local DataStorage = require("datastorage")
        return DataStorage:getDataDir()
    end
end

function FileSyncManager:isRunning()
    return self._running
end

function FileSyncManager:start(silent)
    if self._running then
        if not silent then
            UIManager:show(InfoMessage:new{
                text = _("FileSync server is already running."),
                timeout = 2,
            })
        end
        return
    end

    -- Check WiFi
    if not NetworkMgr:isWifiOn() then
        if not silent then
            UIManager:show(InfoMessage:new{
                text = _("WiFi is not enabled. Please turn on WiFi first."),
                timeout = 3,
            })
        end
        return
    end

    -- Get the local IP
    local ip = self:getLocalIP()
    if not ip then
        if not silent then
            UIManager:show(InfoMessage:new{
                text = _("Could not determine device IP address. Make sure WiFi is connected."),
                timeout = 3,
            })
        end
        return
    end

    local port = self:getPort()
    local root_dir = self:getRootDir()

    -- Start the HTTP server
    local HttpServer = require("httpserver")
    local ok, err = pcall(function()
        self._server = HttpServer:new{
            port = port,
            root_dir = root_dir,
        }
        self._server:start()
    end)

    if not ok then
        logger.err("FileSync: Failed to start server:", err)
        if not silent then
            UIManager:show(InfoMessage:new{
                text = T(_("Failed to start server: %1"), tostring(err)),
                timeout = 5,
            })
        end
        return
    end

    -- Add Kindle firewall rules
    if Device:isKindle() then
        self:openKindleFirewall(port)
    end

    self._running = true
    self._ip = ip
    self._port = port
    logger.info("FileSync: Server started on", ip .. ":" .. port)

    if not silent then
        self:showQRCode()
    end
end

function FileSyncManager:stop(silent)
    if not self._running then
        return
    end

    if self._server then
        pcall(function()
            self._server:stop()
        end)
        self._server = nil
    end

    -- Remove Kindle firewall rules
    if Device:isKindle() then
        self:closeKindleFirewall(self:getPort())
    end

    self._running = false
    logger.info("FileSync: Server stopped")

    if not silent then
        UIManager:show(InfoMessage:new{
            text = _("FileSync server stopped."),
            timeout = 2,
        })
    end
end

function FileSyncManager:showQRCode()
    if not self._running or not self._ip then
        UIManager:show(InfoMessage:new{
            text = _("Server is not running."),
            timeout = 2,
        })
        return
    end

    local url = "http://" .. self._ip .. ":" .. self._port
    local QRMessage = require("ui/widget/qrmessage")
    local Screen = Device.screen
    UIManager:show(QRMessage:new{
        text = url,
        width = Screen:scaleBySize(280),
        height = Screen:scaleBySize(280),
    })
end

function FileSyncManager:openKindleFirewall(port)
    -- Add iptables rule to allow incoming connections on the server port
    os.execute(string.format(
        "iptables -A INPUT -p tcp --dport %d -j ACCEPT 2>/dev/null",
        port
    ))
    logger.info("FileSync: Kindle firewall rule added for port", port)
end

function FileSyncManager:closeKindleFirewall(port)
    -- Remove the iptables rule
    os.execute(string.format(
        "iptables -D INPUT -p tcp --dport %d -j ACCEPT 2>/dev/null",
        port
    ))
    logger.info("FileSync: Kindle firewall rule removed for port", port)
end

return FileSyncManager
