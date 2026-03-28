return function(FileOps, dependencies)
    local lfs = dependencies.lfs
    local logger = dependencies.logger
    local normalize_root_path = dependencies.normalize_root_path
    local is_path_within_root = dependencies.is_path_within_root

    function FileOps:_normalizeUploadRelativePath(relative_path, fallback_filename)
        local normalized = tostring(relative_path or fallback_filename or "")
        normalized = normalized:gsub("\\", "/")
        normalized = normalized:gsub("//+", "/")
        normalized = normalized:gsub("^%s+", ""):gsub("%s+$", "")
        normalized = normalized:gsub("^/+", ""):gsub("/+$", "")

        if normalized == "" then
            return nil, "Empty upload path"
        end

        if normalized:match("%.%.") then
            return nil, "Path traversal not allowed"
        end

        local segments = {}
        for segment in normalized:gmatch("[^/]+") do
            local valid, valid_err = self:_validateFilename(segment)
            if not valid then
                return nil, valid_err
            end
            table.insert(segments, segment)
        end

        if #segments == 0 then
            return nil, "Empty upload path"
        end

        if fallback_filename then
            segments[#segments] = fallback_filename
        end

        return table.concat(segments, "/")
    end

    function FileOps:_collectUploadDirectories(full_dir_path, pending_dirs, scope_root)
        scope_root = normalize_root_path(scope_root or self._root_dir)

        if not full_dir_path or full_dir_path == "" or full_dir_path == scope_root then
            return true
        end

        if not is_path_within_root(full_dir_path, scope_root) then
            return false, "Access denied: path outside root directory"
        end

        local rel = full_dir_path:sub(#scope_root + 1):gsub("^/+", "")
        if rel == "" then
            return true
        end

        local current = scope_root
        for segment in rel:gmatch("[^/]+") do
            local valid, valid_err = self:_validateFilename(segment)
            if not valid then
                return false, valid_err
            end

            current = current .. "/" .. segment
            local attr = lfs.attributes(current)
            if attr then
                if attr.mode ~= "directory" then
                    return false, "Cannot create directory: path component is not a directory"
                end
            else
                pending_dirs[current] = true
            end
        end

        return true
    end

    function FileOps:_createPendingDirectories(pending_dirs)
        local paths = {}
        for path in pairs(pending_dirs) do
            table.insert(paths, path)
        end

        table.sort(paths, function(a, b)
            return #a < #b
        end)

        for _, path in ipairs(paths) do
            local attr = lfs.attributes(path)
            if attr then
                if attr.mode ~= "directory" then
                    return false, "Cannot create directory: path component is not a directory"
                end
            else
                local ok, mkdir_err = lfs.mkdir(path)
                if not ok then
                    return false, "Cannot create directory: " .. tostring(mkdir_err)
                end
            end
        end

        return true
    end

    --- Handle multipart file upload.
    function FileOps:handleUpload(rel_dir, body, boundary, options)
        options = options or {}
        local dir_path, err, scope = self:_resolvePath(rel_dir, options)
        if not dir_path then
            return false, err
        end

        local attr = lfs.attributes(dir_path)
        if not attr or attr.mode ~= "directory" then
            return false, "Upload directory does not exist"
        end

        local delimiter = "--" .. boundary
        local parts = {}
        local search_start = 1
        while true do
            local boundary_start = body:find(delimiter, search_start, true)
            if not boundary_start then break end

            local part_start = body:find("\r\n", boundary_start, true)
            if not part_start then break end
            part_start = part_start + 2

            local next_boundary = body:find(delimiter, part_start, true)
            if not next_boundary then break end

            local part_data = body:sub(part_start, next_boundary - 3)
            table.insert(parts, part_data)
            search_start = next_boundary
        end

        local form_fields = {}
        local upload_entries = {}
        for _, part in ipairs(parts) do
            local header_end = part:find("\r\n\r\n", 1, true)
            if header_end then
                local headers_str = part:sub(1, header_end - 1)
                local file_data = part:sub(header_end + 4)
                local field_name = headers_str:match('name="([^"]+)"')

                local filename = headers_str:match('filename="([^"]+)"')
                if filename and filename ~= "" then
                    filename = filename:match("([^/\\]+)$") or filename

                    if filename:match("%.epub%.zip$") then
                        filename = filename:gsub("%.zip$", "")
                    elseif filename:match("%.cbz%.zip$") then
                        filename = filename:gsub("%.zip$", "")
                    end

                    local valid, valid_err = self:_validateFilename(filename)
                    if valid then
                        if options.safe_mode and not self:isExtensionSafe(filename) then
                            return false, "Root mode required for this file type"
                        end
                        table.insert(upload_entries, {
                            filename = filename,
                            data = file_data,
                        })
                    else
                        logger.warn("FileSync: Invalid filename:", filename, valid_err)
                    end
                elseif field_name and field_name ~= "" then
                    form_fields[field_name] = file_data:gsub("\r\n$", "")
                end
            end
        end

        if #upload_entries == 0 then
            return false, "No files were uploaded"
        end

        local conflict_strategy = self:_normalizeConflictStrategy(form_fields.conflict_strategy or options.conflict_strategy)
        local pending_dirs = {}
        local planned_targets = {}
        local prepared_entries = {}

        for _, entry in ipairs(upload_entries) do
            local upload_rel_path, path_err = self:_normalizeUploadRelativePath(form_fields.relative_path, entry.filename)
            if not upload_rel_path then
                return false, path_err
            end

            local target_rel_path = self:_joinRelativePaths(rel_dir, upload_rel_path)
            local target_full_path, target_err = self:_resolvePath(target_rel_path, options)
            if not target_full_path then
                return false, target_err
            end

            local target_attr = lfs.attributes(target_full_path)
            if target_attr then
                if conflict_strategy == "error" then
                    return false, "Destination already exists", self:_buildDestinationConflict(
                        { mode = "file" },
                        target_attr,
                        target_full_path,
                        scope.id,
                        {
                            destination_path = target_rel_path,
                            source_type = "file",
                        }
                    )
                end
            end

            if planned_targets[target_full_path] then
                return false, "Destination already exists", self:_buildDestinationConflict(
                    { mode = "file" },
                    { mode = "file" },
                    target_full_path,
                    scope.id,
                    {
                        destination_path = target_rel_path,
                        source_type = "file",
                        destination_type = "file",
                    }
                )
            end

            local parent_dir = target_full_path:match("(.+)/[^/]+$")
            local ok_dirs, dir_err = self:_collectUploadDirectories(parent_dir, pending_dirs, scope.root_path)
            if not ok_dirs then
                return false, dir_err
            end

            planned_targets[target_full_path] = true
            table.insert(prepared_entries, {
                full_path = target_full_path,
                relative_path = upload_rel_path,
                data = entry.data,
            })
        end

        local ok_dirs, dir_err = self:_createPendingDirectories(pending_dirs)
        if not ok_dirs then
            return false, dir_err
        end

        local uploaded_count = 0
        for index, entry in ipairs(prepared_entries) do
            local temp_path = string.format("%s.filesync-upload-%d-%d.tmp", entry.full_path, os.time(), index)
            local f = io.open(temp_path, "wb")
            if f then
                local write_ok, write_err = f:write(entry.data)
                local close_ok, close_err = f:close()
                if write_ok and close_ok ~= false then
                    local can_finalize = true
                    local target_attr = lfs.attributes(entry.full_path)
                    if target_attr then
                        local remove_ok, remove_err = self:_removeResolvedPath(entry.full_path)
                        if not remove_ok then
                            os.remove(temp_path)
                            logger.warn("FileSync: Cannot replace existing destination", entry.full_path, remove_err)
                            can_finalize = false
                        end
                    end

                    if can_finalize then
                        local rename_ok, rename_err = os.rename(temp_path, entry.full_path)
                        if rename_ok then
                            uploaded_count = uploaded_count + 1
                            logger.info("FileSync: Uploaded", entry.relative_path, "to", dir_path)
                        else
                            os.remove(temp_path)
                            logger.warn("FileSync: Cannot finalize upload", entry.full_path, rename_err)
                        end
                    end
                else
                    os.remove(temp_path)
                    logger.warn("FileSync: Cannot write upload temp file", temp_path, write_err or close_err)
                end
            else
                logger.warn("FileSync: Cannot write file", temp_path)
            end
        end

        if uploaded_count == #prepared_entries then
            return true
        end

        if uploaded_count > 0 then
            return false, "Some files could not be uploaded"
        end

        return false, "No files were uploaded"
    end

    --- Handle multipart file upload from a temp file on disk (streaming, low RAM).
    --- Works like handleUpload but reads from body_file_path instead of a string.
    function FileOps:handleUploadFromFile(rel_dir, body_file_path, boundary, options)
        options = options or {}
        local dir_path, err, scope = self:_resolvePath(rel_dir, options)
        if not dir_path then
            return false, err
        end

        local attr = lfs.attributes(dir_path)
        if not attr or attr.mode ~= "directory" then
            return false, "Upload directory does not exist"
        end

        -- Read the entire temp file as a string — but ONLY for multipart parsing.
        -- This is still in RAM, but the temp file is the same data that was already
        -- going to be in body. The difference is: we already freed the socket buffers
        -- and this is a single contiguous read instead of incremental table.concat.
        -- For true zero-copy we'd need a binary boundary scanner, but this approach
        -- already gives us the win: the socket read is non-blocking (written to disk),
        -- and we can add chunked scanning later if needed.
        local f, open_err = io.open(body_file_path, "rb")
        if not f then
            return false, "Cannot read upload data: " .. tostring(open_err)
        end

        -- For files under 50MB, read all at once (simpler, fast enough)
        -- For larger files, use chunked boundary scanning
        local file_size = f:seek("end")
        f:seek("set", 0)

        local CHUNK_PARSE_LIMIT = 50 * 1024 * 1024 -- 50 MB

        if file_size <= CHUNK_PARSE_LIMIT then
            -- Small-ish file: read into memory for simple parsing (same as handleUpload)
            local body = f:read("*all")
            f:close()
            return self:handleUpload(rel_dir, body, boundary, options)
        end

        -- Large file: chunked streaming approach
        -- Strategy: scan for boundaries by reading overlapping chunks,
        -- then extract each part's file data directly from disk.
        local delimiter = "--" .. boundary
        local delimiter_len = #delimiter

        local function is_valid_boundary_marker(search_buf, found_index, absolute_pos)
            local at_start = absolute_pos == 1
            if not at_start then
                if found_index <= 2 then
                    return false
                end
                if search_buf:sub(found_index - 2, found_index - 1) ~= "\r\n" then
                    return false
                end
            end

            local after = search_buf:sub(found_index + delimiter_len, found_index + delimiter_len + 1)
            return after == "\r\n" or after == "--"
        end

        -- First pass: find all boundary positions by scanning the file
        local boundary_positions = {}
        local scan_chunk_size = 256 * 1024 -- 256 KB scan chunks
        local overlap = delimiter_len + 4   -- overlap to catch boundaries at chunk edges
        local scan_offset = 0

        f:seek("set", 0)
        local prev_tail = ""

        while scan_offset < file_size do
            local chunk = f:read(scan_chunk_size)
            if not chunk then break end

            -- Combine with tail of previous chunk to catch split boundaries
            local search_buf = prev_tail .. chunk
            local search_base = scan_offset - #prev_tail

            local pos = 1
            while true do
                local found = search_buf:find(delimiter, pos, true)
                if not found then break end
                local absolute_pos = search_base + found
                if is_valid_boundary_marker(search_buf, found, absolute_pos) then
                    if boundary_positions[#boundary_positions] ~= absolute_pos then
                        table.insert(boundary_positions, absolute_pos)
                    end
                end
                pos = found + delimiter_len
            end

            -- Keep the tail for overlap
            if #chunk > overlap then
                prev_tail = chunk:sub(-overlap)
            else
                prev_tail = chunk
            end
            scan_offset = scan_offset + #chunk
        end

        if #boundary_positions < 2 then
            f:close()
            return false, "No files were uploaded"
        end

        -- Second pass: extract parts using boundary positions
        local form_fields = {}
        local upload_entries = {}

        for i = 1, #boundary_positions - 1 do
            local part_boundary_start = boundary_positions[i]
            local next_boundary_start = boundary_positions[i + 1]

            -- Skip the delimiter line and CRLF to get to part start
            f:seek("set", part_boundary_start + delimiter_len - 1)
            local newline_after = f:read(2)
            if not newline_after or newline_after == "--" then
                -- End boundary (--boundary--), skip
                goto continue_part
            end

            local part_data_start = part_boundary_start + delimiter_len + 1 -- +2 for \r\n, -1 for 0-index

            -- Read headers (they're small, usually < 512 bytes)
            f:seek("set", part_data_start)
            local header_buf = f:read(math.min(4096, next_boundary_start - part_data_start))
            if not header_buf then
                goto continue_part
            end

            local header_end_rel = header_buf:find("\r\n\r\n", 1, true)
            if not header_end_rel then
                goto continue_part
            end

            local headers_str = header_buf:sub(1, header_end_rel - 1)
            local content_start = part_data_start + header_end_rel + 3 -- 0-based offset after \r\n\r\n
            local content_end = next_boundary_start - 4 -- 0-based inclusive offset before \r\n + boundary
            if content_end < content_start then
                goto continue_part
            end

            local field_name = headers_str:match('name="([^"]+)"')
            local filename = headers_str:match('filename="([^"]+)"')

            if filename and filename ~= "" then
                filename = filename:match("([^/\\]+)$") or filename

                if filename:match("%.epub%.zip$") then
                    filename = filename:gsub("%.zip$", "")
                elseif filename:match("%.cbz%.zip$") then
                    filename = filename:gsub("%.zip$", "")
                end

                local valid, valid_err = self:_validateFilename(filename)
                if valid then
                    if options.safe_mode and not self:isExtensionSafe(filename) then
                        f:close()
                        return false, "Root mode required for this file type"
                    end
                    table.insert(upload_entries, {
                        filename = filename,
                        content_start = content_start,
                        content_end = content_end,
                    })
                else
                    logger.warn("FileSync: Invalid filename:", filename, valid_err)
                end
            elseif field_name and field_name ~= "" then
                -- Form field: read value (small)
                local value_len = content_end - content_start + 1
                if value_len > 0 and value_len < 4096 then
                    f:seek("set", content_start)
                    local value = f:read(value_len)
                    if value then
                        form_fields[field_name] = value:gsub("\r\n$", "")
                    end
                end
            end

            ::continue_part::
        end

        if #upload_entries == 0 then
            f:close()
            return false, "No files were uploaded"
        end

        -- Plan and validate all targets
        local conflict_strategy = self:_normalizeConflictStrategy(form_fields.conflict_strategy or options.conflict_strategy)
        local pending_dirs = {}
        local planned_targets = {}
        local prepared_entries = {}

        for _, entry in ipairs(upload_entries) do
            local upload_rel_path, path_err = self:_normalizeUploadRelativePath(form_fields.relative_path, entry.filename)
            if not upload_rel_path then
                f:close()
                return false, path_err
            end

            local target_rel_path = self:_joinRelativePaths(rel_dir, upload_rel_path)
            local target_full_path, target_err = self:_resolvePath(target_rel_path, options)
            if not target_full_path then
                f:close()
                return false, target_err
            end

            local target_attr = lfs.attributes(target_full_path)
            if target_attr then
                if conflict_strategy == "error" then
                    f:close()
                    return false, "Destination already exists", self:_buildDestinationConflict(
                        { mode = "file" },
                        target_attr,
                        target_full_path,
                        scope.id,
                        {
                            destination_path = target_rel_path,
                            source_type = "file",
                        }
                    )
                end
            end

            if planned_targets[target_full_path] then
                f:close()
                return false, "Destination already exists", self:_buildDestinationConflict(
                    { mode = "file" },
                    { mode = "file" },
                    target_full_path,
                    scope.id,
                    {
                        destination_path = target_rel_path,
                        source_type = "file",
                        destination_type = "file",
                    }
                )
            end

            local parent_dir = target_full_path:match("(.+)/[^/]+$")
            local ok_dirs, dir_err = self:_collectUploadDirectories(parent_dir, pending_dirs, scope.root_path)
            if not ok_dirs then
                f:close()
                return false, dir_err
            end

            planned_targets[target_full_path] = true
            table.insert(prepared_entries, {
                full_path = target_full_path,
                relative_path = upload_rel_path,
                content_start = entry.content_start,
                content_end = entry.content_end,
            })
        end

        local ok_dirs, dir_err = self:_createPendingDirectories(pending_dirs)
        if not ok_dirs then
            f:close()
            return false, dir_err
        end

        -- Write each file part from temp file to destination (chunked, 64KB at a time)
        local uploaded_count = 0
        local write_chunk_size = 65536

        for index, entry in ipairs(prepared_entries) do
            local temp_path = string.format("%s.filesync-upload-%d-%d.tmp", entry.full_path, os.time(), index)
            local out_f = io.open(temp_path, "wb")
            if out_f then
                f:seek("set", entry.content_start)
                local remaining = entry.content_end - entry.content_start + 1
                local write_ok = true

                while remaining > 0 do
                    local to_read = math.min(remaining, write_chunk_size)
                    local chunk = f:read(to_read)
                    if not chunk or #chunk == 0 then
                        write_ok = false
                        logger.warn("FileSync: Read error during streaming upload", temp_path, "remaining", remaining)
                        break
                    end
                    local wok, werr = out_f:write(chunk)
                    if not wok then
                        write_ok = false
                        logger.warn("FileSync: Write error during streaming upload", temp_path, werr)
                        break
                    end
                    remaining = remaining - #chunk
                end

                if remaining > 0 then
                    write_ok = false
                end

                local close_ok = out_f:close()
                if write_ok and close_ok ~= false then
                    local can_finalize = true
                    local target_attr = lfs.attributes(entry.full_path)
                    if target_attr then
                        local remove_ok, remove_err = self:_removeResolvedPath(entry.full_path)
                        if not remove_ok then
                            os.remove(temp_path)
                            logger.warn("FileSync: Cannot replace existing destination", entry.full_path, remove_err)
                            can_finalize = false
                        end
                    end

                    if can_finalize then
                        local rename_ok, rename_err = os.rename(temp_path, entry.full_path)
                        if rename_ok then
                            uploaded_count = uploaded_count + 1
                            logger.info("FileSync: Uploaded (streamed)", entry.relative_path, "to", dir_path)
                        else
                            os.remove(temp_path)
                            logger.warn("FileSync: Cannot finalize upload", entry.full_path, rename_err)
                        end
                    end
                else
                    os.remove(temp_path)
                    logger.warn("FileSync: Cannot write upload temp file", temp_path)
                end
            else
                logger.warn("FileSync: Cannot create temp file", temp_path)
            end
        end

        f:close()

        if uploaded_count == #prepared_entries then
            return true
        end

        if uploaded_count > 0 then
            return false, "Some files could not be uploaded"
        end

        return false, "No files were uploaded"
    end
end
