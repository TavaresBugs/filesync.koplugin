local WidgetContainer = require("ui/widget/container/widgetcontainer")
local _ = require("gettext")

local FileSync = WidgetContainer:extend{
    name = "FileSync",
    is_doc_only = false,
}

function FileSync:init()
    self.ui.menu:registerToMainMenu(self)
end

function FileSync:addToMainMenu(menu_items)
    menu_items.filesync = {
        text = _("FileSync"),
        sorting_hint = "network",
        sub_item_table = {
            {
                text_func = function()
                    local FileSyncManager = require("filesyncmanager")
                    if FileSyncManager:isRunning() then
                        return _("Stop file server")
                    else
                        return _("Start file server")
                    end
                end,
                callback = function()
                    local FileSyncManager = require("filesyncmanager")
                    if FileSyncManager:isRunning() then
                        FileSyncManager:stop()
                    else
                        FileSyncManager:start()
                    end
                end,
                keep_menu_open = false,
            },
            {
                text = _("Server port"),
                callback = function()
                    local FileSyncManager = require("filesyncmanager")
                    FileSyncManager:configurePort()
                end,
                keep_menu_open = true,
            },
            {
                text = _("Safe mode"),
                checked_func = function()
                    local FileSyncManager = require("filesyncmanager")
                    return FileSyncManager:getSafeMode()
                end,
                callback = function()
                    local FileSyncManager = require("filesyncmanager")
                    FileSyncManager:setSafeMode(not FileSyncManager:getSafeMode())
                end,
                keep_menu_open = true,
            },
            {
                text = _("Show QR code"),
                enabled_func = function()
                    local FileSyncManager = require("filesyncmanager")
                    return FileSyncManager:isRunning()
                end,
                callback = function()
                    local FileSyncManager = require("filesyncmanager")
                    FileSyncManager:showQRCode()
                end,
                keep_menu_open = false,
            },
            {
                text = _("About"),
                callback = function()
                    local UIManager = require("ui/uimanager")
                    local InfoMessage = require("ui/widget/infomessage")
                    UIManager:show(InfoMessage:new{
                        text = _("FileSync v1.0.0\n\nWireless file manager for KOReader.\n\nStart the server, scan the QR code with your phone, and manage your books from any browser on the same WiFi network."),
                    })
                end,
                keep_menu_open = true,
            },
        },
    }
end

function FileSync:onSuspend()
    local FileSyncManager = require("filesyncmanager")
    if FileSyncManager:isRunning() then
        FileSyncManager._was_running_before_suspend = true
        FileSyncManager:stop(true) -- silent stop
    end
end

function FileSync:onResume()
    local FileSyncManager = require("filesyncmanager")
    if FileSyncManager._was_running_before_suspend then
        FileSyncManager._was_running_before_suspend = false
        FileSyncManager:start(true) -- silent start (no QR code)
    end
end

function FileSync:onEnterStandby()
    self:onSuspend()
end

function FileSync:onLeaveStandby()
    self:onResume()
end

function FileSync:onExit()
    local FileSyncManager = require("filesyncmanager")
    if FileSyncManager:isRunning() then
        FileSyncManager:stop(true)
    end
end

return FileSync
