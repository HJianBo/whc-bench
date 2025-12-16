-- wrk lua 脚本 - EMQX 模式
-- 使用方法: wrk -s wrk_emqx.lua -t 4 -c 100 -d 30s --timeout 30s -H "Authorization: Basic YOUR_SK" http://your-url

-- 设备 ID 列表（将从 CSV 文件读取或使用默认值）
local device_ids = {}

-- CSV 文件路径（可通过命令行参数传入）
local csv_file = "devices.csv"

-- 每个设备的请求计数器（用于 mid）
local device_counters = {}

-- 请求统计（使用全局变量以便在 done() 中访问）
failed_requests = {}
failed_count = 0
success_count = 0
total_count = 0
status_code_counts = {}
all_request_samples = {}  -- 保存所有请求样本
max_failed_samples = 10  -- 最多保存的失败请求样本数
max_all_samples = 20  -- 最多保存的所有请求样本数

-- 从 CSV 文件读取 device IDs
local function load_device_ids_from_csv(file_path)
    local ids = {}
    local file = io.open(file_path, "r")
    if not file then
        return nil, "无法打开文件: " .. file_path
    end
    
    local is_first_line = true
    local device_id_column = nil
    
    for line in file:lines() do
        -- 跳过空行
        if line and string.match(line, "%S") then
            -- 解析 CSV 行（简单实现，不支持引号内的逗号）
            local fields = {}
            for field in string.gmatch(line, "([^,]+)") do
                -- 去除首尾空白
                field = string.match(field, "^%s*(.-)%s*$")
                table.insert(fields, field)
            end
            
            if is_first_line then
                -- 第一行是表头，查找 deviceId 列
                is_first_line = false
                for i, header in ipairs(fields) do
                    if string.lower(header) == "deviceid" then
                        device_id_column = i
                        break
                    end
                end
                -- 如果没有找到 deviceId 列，使用第一列
                if not device_id_column then
                    device_id_column = 1
                end
            else
                -- 数据行，提取 device ID
                if fields[device_id_column] and fields[device_id_column] ~= "" then
                    table.insert(ids, fields[device_id_column])
                end
            end
        end
    end
    
    file:close()
    return ids
end

-- 初始化函数：在脚本加载时执行一次
function init(args)
    -- 尝试从命令行参数获取 CSV 文件路径
    if args and #args > 0 then
        csv_file = args[1]
    end
    
    -- 尝试从 CSV 文件读取 device IDs
    local loaded_ids, err = load_device_ids_from_csv(csv_file)
    if loaded_ids and #loaded_ids > 0 then
        device_ids = loaded_ids
    else
        if #device_ids == 0 then
            error("device_ids 列表为空，请确保 CSV 文件存在或使用 generate_wrk_script.py 生成脚本")
        end
    end
    
    -- 初始化每个设备的计数器
    for i, device_id in ipairs(device_ids) do
        device_counters[device_id] = 0
    end
end

-- 生成 UUID v4（32 位十六进制字符串，无连字符）
function generate_uuid()
    local template = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    local random = math.random
    local uuid = string.gsub(template, "x", function()
        return string.format("%x", random(0, 15))
    end)
    return uuid
end

-- 获取当前时间戳（秒，带小数部分）
local function get_current_time()
    if wrk and wrk.time then
        return wrk.time()
    else
        -- 后备方案：使用 os.time()，但没有毫秒精度
        return os.time()
    end
end

-- 生成纳秒级时间戳（lua 只支持秒级，这里返回秒*1e9的数字）
function get_timestamp_ns()
    local ts = get_current_time()
    -- 返回纳秒时间戳（作为数字）
    -- 注意：lua 的数字精度有限，但对于时间戳来说通常足够
    return math.floor(ts * 1000000000)
end

-- 生成 ISO 8601 格式的时间（+08:00 时区）
function get_event_time()
    -- 获取当前 UTC 时间戳
    local utc_ts = os.time()
    -- 转换为 UTC+8 时区
    local ts = utc_ts + 8 * 3600
    -- 格式化日期时间（使用 UTC 格式）
    local date = os.date("!%Y-%m-%dT%H:%M:%S", ts)
    -- 获取毫秒部分（使用 get_current_time() 的小数部分）
    local current_time = get_current_time()
    local ts_frac = current_time % 1
    local ms = math.floor(ts_frac * 1000)
    return string.format("%s.%03d+08:00", date, ms)
end

-- 请求函数：为每个请求生成请求体
function request()
    -- 检查 device_ids 是否为空
    if #device_ids == 0 then
        error("device_ids 列表为空，请先使用 generate_wrk_script.py 生成脚本")
    end
    
    -- 随机选择一个设备 ID
    local device_id = device_ids[math.random(#device_ids)]
    
    -- 初始化计数器（如果不存在）
    if device_counters[device_id] == nil then
        device_counters[device_id] = 0
    end
    
    -- 增加该设备的计数器
    device_counters[device_id] = device_counters[device_id] + 1
    local mid = device_counters[device_id]
    
    -- 生成时间戳和 UUID
    local timestamp_ns = get_timestamp_ns()
    local msg_id = generate_uuid()
    local event_time = get_event_time()
    
    -- 构建 payload_data
    local payload_data = {
        cmd = "bench",
        deviceId = device_id,
        eventTime = event_time,
        expire = 5,
        mid = mid,
        msgId = msg_id,
        paras = {
            timestamp = timestamp_ns
        },
        serviceId = "bench"
    }
    
    -- 构建 EMQX 消息体
    local emqx_payload = {
        topic = "/v1/devices/" .. device_id .. "/command",
        retain = false,
        qos = 1,
        properties = {
            user_properties = {
                businessID = device_id .. "_" .. mid
            },
            message_expiry_interval = 5
        },
        payload_encoding = "plain",
        payload = json_encode(payload_data)
    }
    
    -- 返回请求
    return wrk.format("POST", nil, nil, json_encode(emqx_payload))
end

-- JSON 编码函数（简化版，用于生成 JSON 字符串）
function json_encode(obj)
    if type(obj) == "table" then
        local parts = {}
        local is_array = true
        local max_index = 0
        
        -- 检查是否是数组
        for k, v in pairs(obj) do
            if type(k) ~= "number" then
                is_array = false
                break
            end
            if k > max_index then
                max_index = k
            end
        end
        
        if is_array and max_index == #obj then
            -- 数组
            for i, v in ipairs(obj) do
                table.insert(parts, json_encode(v))
            end
            return "[" .. table.concat(parts, ",") .. "]"
        else
            -- 对象
            for k, v in pairs(obj) do
                local key = json_encode(tostring(k))
                local value = json_encode(v)
                table.insert(parts, key .. ":" .. value)
            end
            return "{" .. table.concat(parts, ",") .. "}"
        end
    elseif type(obj) == "string" then
        -- 转义特殊字符
        obj = string.gsub(obj, "\\", "\\\\")
        obj = string.gsub(obj, '"', '\\"')
        obj = string.gsub(obj, "\n", "\\n")
        obj = string.gsub(obj, "\r", "\\r")
        obj = string.gsub(obj, "\t", "\\t")
        return '"' .. obj .. '"'
    elseif type(obj) == "number" then
        -- 对于大数字，使用科学计数法避免精度丢失
        if obj >= 1e15 then
            return string.format("%.0f", obj)
        else
            return tostring(obj)
        end
    elseif type(obj) == "boolean" then
        return obj and "true" or "false"
    elseif obj == nil then
        return "null"
    else
        return '"' .. tostring(obj) .. '"'
    end
end

-- 响应处理函数：处理每个响应
function response(status, headers, body)
    -- 确保变量已初始化
    if total_count == nil then total_count = 0 end
    if status_code_counts == nil then status_code_counts = {} end
    if all_request_samples == nil then all_request_samples = {} end
    if failed_requests == nil then failed_requests = {} end
    if failed_count == nil then failed_count = 0 end
    if success_count == nil then success_count = 0 end
    
    total_count = total_count + 1
    
    -- 统计状态码
    status_code_counts[status] = (status_code_counts[status] or 0) + 1
    
    -- 保存请求样本（最多保存 max_all_samples 个）
    if #all_request_samples < max_all_samples then
        local sample = {
            status = status,
            body = body or "",
            is_success = status >= 200 and status < 400
        }
        table.insert(all_request_samples, sample)
    end
    
    -- 检查是否是失败响应（4xx 和 5xx）
    if status >= 400 then
        failed_count = failed_count + 1
        
        -- 保存失败请求样本（最多保存 max_failed_samples 个）
        if #failed_requests < max_failed_samples then
            local sample = {
                status = status,
                body = body or ""
            }
            table.insert(failed_requests, sample)
        end
    else
        success_count = success_count + 1
    end
end

-- 测试结束函数：打印所有请求统计和响应样本
function done(summary, latency, requests)
    io.write("\n")
    io.write("=" .. string.rep("=", 70) .. "\n")
    io.write("请求统计报告\n")
    io.write("=" .. string.rep("=", 70) .. "\n")
    
    -- 使用 response() 函数收集的统计信息
    local actual_total = total_count or 0
    local actual_success = success_count or 0
    local actual_failed = failed_count or 0
    
    -- 安全地检查 summary 结构
    local summary_requests_complete = nil
    if summary then
        if type(summary) == "table" then
            if summary.requests then
                if type(summary.requests) == "table" then
                    summary_requests_complete = summary.requests.complete
                elseif type(summary.requests) == "number" then
                    summary_requests_complete = summary.requests
                end
            end
        end
    end
    
    -- 如果 response 函数没有被调用，尝试从 summary 获取信息
    if actual_total == 0 and summary_requests_complete then
        actual_total = summary_requests_complete
        actual_failed = actual_total
        actual_success = 0
    end
    
    -- 总体统计
    io.write(string.format("\n总请求数: %d\n", actual_total))
    io.write(string.format("成功请求: %d (%.2f%%)\n", actual_success, 
        actual_total > 0 and (actual_success / actual_total) * 100 or 0))
    io.write(string.format("失败请求: %d (%.2f%%)\n", actual_failed, 
        actual_total > 0 and (actual_failed / actual_total) * 100 or 0))
    io.write("\n")
    
    -- 状态码分布统计
    if next(status_code_counts) then
        io.write("状态码分布:\n")
        local sorted_status = {}
        for status, _ in pairs(status_code_counts) do
            table.insert(sorted_status, status)
        end
        table.sort(sorted_status)
        
        for _, status in ipairs(sorted_status) do
            local count = status_code_counts[status]
            local percentage = total_count > 0 and (count / total_count) * 100 or 0
            local status_label = ""
            if status >= 200 and status < 300 then
                status_label = " (成功)"
            elseif status >= 300 and status < 400 then
                status_label = " (重定向)"
            elseif status >= 400 and status < 500 then
                status_label = " (客户端错误)"
            elseif status >= 500 then
                status_label = " (服务器错误)"
            end
            io.write(string.format("  %d%s: %d 次 (%.2f%%)\n", 
                status, status_label, count, percentage))
        end
        io.write("\n")
    end
    
    -- 打印所有请求样本（成功和失败都打印完整响应体）
    if all_request_samples and #all_request_samples > 0 then
        io.write("=" .. string.rep("=", 70) .. "\n")
        io.write("请求响应样本\n")
        io.write("=" .. string.rep("=", 70) .. "\n")
        io.write(string.format("\n请求样本 (显示前 %d 个):\n", #all_request_samples))
        io.write("-" .. string.rep("-", 70) .. "\n")
        for i, sample in ipairs(all_request_samples) do
            local status_icon = sample.is_success and "✓" or "✗"
            local status_label = sample.is_success and "成功" or "失败"
            io.write(string.format("\n[样本 %d] %s 状态码: %d (%s)\n", i, status_icon, sample.status, status_label))
            if sample.body and sample.body ~= "" then
                io.write("响应体:\n")
                io.write(sample.body .. "\n")
            else
                io.write("响应体: (无响应体)\n")
            end
            io.write("-" .. string.rep("-", 70) .. "\n")
        end
        io.write("\n")
    end
    
    io.write("\n")
end

