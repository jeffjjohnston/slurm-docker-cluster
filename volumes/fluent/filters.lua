-- nf_enrich.lua
meta = meta or {}
file_list = file_list or {}
MAX_FILES = 10000

local function add_file(file)
    if meta[file] ~= nil then return end
    file_list[#file_list + 1] = file
    if #file_list > MAX_FILES then
        local oldest = table.remove(file_list, 1)
        meta[oldest] = nil
    end
end

function nf_parse_and_enrich(tag, ts, record)
    local file = record["file"]
    local log  = record["log"]

    if not file or not log then
        return 1, ts, record
    end

    -- Try to parse as JSON (first line with metadata)
    local success, decoded = pcall(function()
        -- Simple JSON detection and parsing
        if log:match("^%s*{") then
            local result = {}
            for k, v in log:gmatch('"([^"]+)"%s*:%s*"([^"]*)"') do
                result[k] = v
            end
            return result
        end
        return nil
    end)

    if success and decoded and next(decoded) then
        -- Store metadata for this file
        meta[file] = decoded
        add_file(file)
        -- Add metadata to current record
        --for k, v in pairs(decoded) do
        --    record[k] = v
        --end
        --record["nf_meta"] = true
        return -1, ts, record
    else
        -- Regular log line - apply stored metadata
        local m = meta[file]
        if m then
            for k, v in pairs(m) do
                record[k] = v
            end
        end
    end

    -- Determine stream from filename
    if file:match("%.command%.err$") then
        record["stream"] = "stderr"
    else
        record["stream"] = "stdout"
    end

    record["source"] = "nextflow"

    return 1, ts, record
end
