-- wrk lua 脚本 - EMQX 模式
-- 使用方法: wrk -s wrk_emqx.lua -t 4 -c 100 -d 30s --timeout 30s -H "Authorization: Basic YOUR_SK" http://your-url

-- 设备 ID 列表（将在生成脚本时填充）
local device_ids = {}

-- 每个设备的请求计数器（用于 mid）
local device_counters = {}

-- 初始化函数：在脚本加载时执行一次
function init(args)
    -- device_ids 已在脚本中定义
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

