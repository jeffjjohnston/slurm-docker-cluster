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

function enrich_nextflow_metadata(tag, ts, record)
    local file = record["file"]

    -- DEBUG: Uncomment to see what fluent bit is actually passing
    -- print("File: " .. (file or "nil") .. " Dump: ")
    -- for k, v in pairs(record) do print(k, v) end

    if not file then
        return 1, ts, record
    end

    -- Detect if record contains metadata parsed by the JSON parser
    local v = record["nf_meta"]

    -- Check for boolean true or string "true"
    if v == true or v == "true" then
        -- print("Storing metadata for file: " .. file)
        -- We create a clean copy of metadata, excluding the 'log' key and 'file' keys
        local clean_meta = {}
        for k, val in pairs(record) do
            if k ~= "log" and k ~= "file" and k ~= "nf_meta" then
                clean_meta[k] = val
            end
        end

        meta[file] = clean_meta
        add_file(file)
        -- discard this line as it only contains metadata
        return -1, ts, record
    end

    -- For other lines, reuse stored metadata
    local m = meta[file]
    if m then
        -- print("Enriching log line for file: " .. file)
        for k, v in pairs(m) do
            record[k] = v
        end
    end

    -- Assign stream based on file name: .command.err is stderr, others are stdout
    if file:match("%.command%.err$") then
        record["stream"] = "stderr"
    else
        record["stream"] = "stdout"
    end

    record["source"] = "nextflow"

    return 1, ts, record
end
