-- Custom gettext for FileSync plugin
-- Loads .po translations from the plugin's i18n/ directory
-- Falls back to English (returns msgid) if no translation found

local translations = {}
local loaded = false

local function loadTranslations()
    if loaded then return end
    loaded = true

    -- Get current language from KOReader settings
    local lang = G_reader_settings:readSetting("language") or "en"
    if lang == "en" or lang == "C" or lang == "" then return end

    -- Find the plugin directory
    local info = debug.getinfo(1, "S")
    local script_path = info.source:match("@(.+)")
    local plugin_dir = script_path and script_path:match("(.+)/[^/]+$") or "."

    -- Try exact match first (e.g., "es_ES.po"), then base language (e.g., "es.po")
    local po_path = plugin_dir .. "/i18n/" .. lang .. ".po"
    local f = io.open(po_path, "r")
    if not f then
        local base_lang = lang:match("^([^_]+)")
        if base_lang and base_lang ~= lang then
            po_path = plugin_dir .. "/i18n/" .. base_lang .. ".po"
            f = io.open(po_path, "r")
        end
    end
    if not f then return end

    local content = f:read("*all")
    f:close()

    if not content or #content == 0 then return end

    local function unescape(s)
        return s:gsub("\\n", "\n"):gsub("\\t", "\t"):gsub('\\"', '"'):gsub("\\\\", "\\")
    end

    -- Parse .po file: extract msgid/msgstr pairs
    local current_msgid = nil
    local current_msgstr = nil
    local state = nil -- "msgid" or "msgstr"

    local function storePair()
        if current_msgid and current_msgid ~= "" and current_msgstr and current_msgstr ~= "" then
            translations[current_msgid] = current_msgstr
        end
        current_msgid = nil
        current_msgstr = nil
        state = nil
    end

    for line in (content .. "\n"):gmatch("([^\n]*)\n") do
        -- Trim carriage return
        line = line:gsub("\r$", "")

        if line:match("^#") then
            -- Comment line, skip
        elseif line:match("^msgid%s+\"(.-)\"$") then
            -- New msgid: store any previous pair first
            storePair()
            current_msgid = unescape(line:match("^msgid%s+\"(.-)\"$"))
            state = "msgid"
        elseif line:match("^msgstr%s+\"(.-)\"$") then
            current_msgstr = unescape(line:match("^msgstr%s+\"(.-)\"$"))
            state = "msgstr"
        elseif line:match("^\"(.-)\"$") then
            -- Continuation line
            local cont = unescape(line:match("^\"(.-)\"$"))
            if state == "msgid" then
                current_msgid = (current_msgid or "") .. cont
            elseif state == "msgstr" then
                current_msgstr = (current_msgstr or "") .. cont
            end
        end
        -- Empty lines and unrecognized lines are simply ignored;
        -- pairs are stored when the next msgid is encountered or at EOF
    end

    -- Store the last pair
    storePair()
end

local function gettext(msgid)
    loadTranslations()
    return translations[msgid] or msgid
end

return gettext
