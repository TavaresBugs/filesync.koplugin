local Blitbuffer = require("ffi/blitbuffer")
local CenterContainer = require("ui/widget/container/centercontainer")
local ConfirmBox = require("ui/widget/confirmbox")
local Device = require("device")
local FrameContainer = require("ui/widget/container/framecontainer")
local Geom = require("ui/geometry")
local GestureRange = require("ui/gesturerange")
local InfoMessage = require("ui/widget/infomessage")
local InputContainer = require("ui/widget/container/inputcontainer")
local NetworkMgr = require("ui/network/manager")
local OverlapGroup = require("ui/widget/overlapgroup")
local QRWidget = require("ui/widget/qrwidget")
local RightContainer = require("ui/widget/container/rightcontainer")
local Size = require("ui/size")
local TextBoxWidget = require("ui/widget/textboxwidget")
local TextWidget = require("ui/widget/textwidget")
local UIManager = require("ui/uimanager")
local VerticalGroup = require("ui/widget/verticalgroup")
local VerticalSpan = require("ui/widget/verticalspan")
local Font = require("ui/font")
local HorizontalGroup = require("ui/widget/horizontalgroup")
local HorizontalSpan = require("ui/widget/horizontalspan")
local ImageWidget = require("ui/widget/imagewidget")
local logger = require("logger")
local Screen = Device.screen
local ok_i18n, plugin_gettext = pcall(require, "filesync/filesync_i18n")
local _ = ok_i18n and plugin_gettext or require("gettext")
local T = require("ffi/util").template

local FileSyncManager = {
    _running = false,
    _server = nil,
    _port = nil,
    _ip = nil,
    _was_running_before_suspend = false,
    _standby_prevented = false,
    _qr_widget = nil,
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
    local HttpServer = require("filesync/httpserver")
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
    self:preventStandby()
    logger.info("FileSync: Server started on", ip .. ":" .. port)

    if not silent then
        self:showQRCode()
    end
end

function FileSyncManager:stop(silent)
    if not self._running then
        return
    end

    -- Close QR screen if open
    self:closeQRScreen()

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
    self:allowStandby()
    logger.info("FileSync: Server stopped")

    if not silent then
        UIManager:show(InfoMessage:new{
            text = _("FileSync server stopped."),
            timeout = 2,
        })
        UIManager:restartKOReader()
    end
end

function FileSyncManager:preventStandby()
    if not self._standby_prevented then
        UIManager:preventStandby()
        self._standby_prevented = true
        logger.info("FileSync: Standby prevented")
    end
end

function FileSyncManager:allowStandby()
    if self._standby_prevented then
        UIManager:allowStandby()
        self._standby_prevented = false
        logger.info("FileSync: Standby allowed")
    end
end

function FileSyncManager:checkBatteryAndStart()
    local ok_power, power_device = pcall(function() return Device:getPowerDevice() end)
    local capacity = 100
    local is_charging = false
    if ok_power and power_device then
        local ok_cap, cap = pcall(function() return power_device:getCapacity() end)
        if ok_cap and cap then capacity = cap end
        local ok_chg, chg = pcall(function() return power_device:isCharging() end)
        if ok_chg then is_charging = chg end
    end

    if capacity < 15 and not is_charging then
        UIManager:show(ConfirmBox:new{
            title = _("Low Battery"),
            text = T(_("Battery level is at %1%. Running the server may drain the battery quickly."), capacity),
            ok_text = _("Start Anyway"),
            cancel_text = _("Cancel"),
            ok_callback = function()
                self:start()
            end,
        })
    else
        self:start()
    end
end

function FileSyncManager:closeQRScreen()
    if self._qr_widget then
        UIManager:close(self._qr_widget, "full")
        self._qr_widget = nil
    end
end

function FileSyncManager:_showQRCodeOpenError(err)
    logger.err("FileSync: Failed to open QR screen:", err)
    UIManager:show(InfoMessage:new{
        text = T(_("Failed to open QR screen:\n%1"), tostring(err)),
    })
end

function FileSyncManager:showQRCode()
    if not self._running or not self._ip then
        UIManager:show(InfoMessage:new{
            text = _("Server is not running."),
            timeout = 2,
        })
        return
    end

    local ok, err = pcall(function()
        self:closeQRScreen()

        local url = "http://" .. self._ip .. ":" .. self._port
        local screen_width = Screen:getWidth()
        local screen_height = Screen:getHeight()
        local full_screen_dimen = Geom:new{ x = 0, y = 0, w = screen_width, h = screen_height }

        local qr_size = Screen:scaleBySize(260)
        local qr_widget = QRWidget:new{
            text = url,
            width = qr_size,
            height = qr_size,
        }

        local title_text = TextWidget:new{
            text = _("FileSync"),
            face = Font:getFace("infofont", 34),
            bold = true,
            fgcolor = Blitbuffer.COLOR_BLACK,
        }
        local title_widget = title_text

        local source = debug.getinfo(1, "S").source or ""
        local icon_dir = source:match("@(.*/)") or ""
        if icon_dir ~= "" then
            local icon_size = Screen:scaleBySize(46)
            local ok_icon, icon_widget = pcall(function()
                return ImageWidget:new{
                    file = icon_dir .. "icon.png",
                    width = icon_size,
                    height = icon_size,
                    alpha = true,
                }
            end)
            if ok_icon and icon_widget then
                title_widget = HorizontalGroup:new{
                    align = "center",
                    icon_widget,
                    HorizontalSpan:new{ width = Screen:scaleBySize(8) },
                    title_text,
                }
            else
                logger.warn("FileSync: Failed to load QR header icon:", icon_widget)
            end
        end

        local url_widget = TextWidget:new{
            text = url,
            face = Font:getFace("infofont", 22),
            fgcolor = Blitbuffer.COLOR_BLACK,
            max_width = screen_width - Screen:scaleBySize(40),
        }

        local instructions_widget = TextBoxWidget:new{
            text = _("Scan the QR code or enter the URL\nin your browser.\n\nBoth devices must be on the same WiFi network."),
            face = Font:getFace("smallinfofont", 20),
            width = math.floor(screen_width * 0.65),
            alignment = "center",
            fgcolor = Blitbuffer.COLOR_BLACK,
        }

        local button_text = TextWidget:new{
            text = _("Stop Server"),
            face = Font:getFace("infofont", 20),
            fgcolor = Blitbuffer.COLOR_BLACK,
        }
        local stop_button = FrameContainer:new{
            bordersize = Size.border.button,
            radius = Size.radius.button,
            padding = Screen:scaleBySize(10),
            padding_left = Screen:scaleBySize(30),
            padding_right = Screen:scaleBySize(30),
            background = Blitbuffer.COLOR_WHITE,
            button_text,
        }

        local close_button_box_size = Screen:scaleBySize(40)
        local close_button_text = TextWidget:new{
            text = "\u{00D7}",
            face = Font:getFace("infofont", 30),
            fgcolor = Blitbuffer.COLOR_BLACK,
        }
        local close_button = FrameContainer:new{
            bordersize = Size.border.button,
            radius = Screen:scaleBySize(8),
            padding = 0,
            background = Blitbuffer.COLOR_WHITE,
            CenterContainer:new{
                dimen = Geom:new{
                    w = close_button_box_size,
                    h = close_button_box_size,
                },
                close_button_text,
            },
        }

        local vertical_content = VerticalGroup:new{
            align = "center",
            VerticalSpan:new{ width = Screen:scaleBySize(40) },
            title_widget,
            VerticalSpan:new{ width = Screen:scaleBySize(30) },
            qr_widget,
            VerticalSpan:new{ width = Screen:scaleBySize(20) },
            url_widget,
            VerticalSpan:new{ width = Screen:scaleBySize(15) },
            instructions_widget,
            VerticalSpan:new{ width = Screen:scaleBySize(30) },
            stop_button,
        }

        local close_button_row = RightContainer:new{
            dimen = Geom:new{
                x = 0,
                y = 0,
                w = screen_width - Screen:scaleBySize(10),
                h = close_button_box_size + Screen:scaleBySize(10),
            },
            FrameContainer:new{
                bordersize = 0,
                padding = 0,
                padding_top = Screen:scaleBySize(10),
                padding_right = Screen:scaleBySize(10),
                background = Blitbuffer.COLOR_WHITE,
                close_button,
            },
        }

        local centered_content = CenterContainer:new{
            dimen = full_screen_dimen,
            vertical_content,
        }

        local overlap = OverlapGroup:new{
            dimen = full_screen_dimen,
            centered_content,
            close_button_row,
        }

        local frame = FrameContainer:new{
            width = screen_width,
            height = screen_height,
            bordersize = 0,
            padding = 0,
            margin = 0,
            background = Blitbuffer.COLOR_WHITE,
            overlap,
        }

        local widget = InputContainer:new{
            dimen = full_screen_dimen,
            width = screen_width,
            height = screen_height,
        }
        widget[1] = frame
        widget._stop_button = stop_button
        widget._close_button = close_button
        widget._manager = self

        widget.ges_events = {
            Tap = {
                GestureRange:new{
                    ges = "tap",
                    range = full_screen_dimen,
                },
            },
        }

        function widget:onTap(_event, ges)
            if not ges then return true end
            local x, y = ges.pos.x, ges.pos.y

            local stop_btn = self._stop_button
            if stop_btn and stop_btn.dimen then
                if x >= stop_btn.dimen.x and x <= stop_btn.dimen.x + stop_btn.dimen.w
                   and y >= stop_btn.dimen.y and y <= stop_btn.dimen.y + stop_btn.dimen.h then
                    self._manager:closeQRScreen()
                    UIManager:show(InfoMessage:new{
                        text = _("Stopping server..."),
                        timeout = 2,
                    })
                    UIManager:scheduleIn(0.5, function()
                        self._manager:stop(true)
                        UIManager:restartKOReader()
                    end)
                    return true
                end
            end

            local close_btn = self._close_button
            if close_btn and close_btn.dimen then
                if x >= close_btn.dimen.x and x <= close_btn.dimen.x + close_btn.dimen.w
                   and y >= close_btn.dimen.y and y <= close_btn.dimen.y + close_btn.dimen.h then
                    self._manager:closeQRScreen()
                    UIManager:show(InfoMessage:new{
                        text = _("Server running in the background. Stop the server to save battery."),
                        timeout = 4,
                    })
                    return true
                end
            end

            return true
        end

        function widget:onClose()
            return true
        end

        self._qr_widget = widget
        UIManager:show(widget, "full")
    end)

    if not ok then
        self._qr_widget = nil
        self:_showQRCodeOpenError(err)
    end
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
