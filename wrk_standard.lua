-- wrk lua 脚本 - 标准模式
-- 使用方法: wrk -s wrk_standard.lua -t 4 -c 100 -d 30s --timeout 30s -H "edgeNodeId: YOUR_EDGE_NODE_ID" http://your-url

-- 设备 ID 列表（将在生成脚本时填充）
local device_ids = {}

-- 产品 ID（可选，将在生成脚本时填充）
local product_id = nil

-- 初始化函数：在脚本加载时执行一次
function init(args)
    -- device_ids 和 product_id 已在脚本中定义
end

-- 生成纳秒级时间戳（lua 只支持秒级，这里返回秒*1e9的数字）
function get_timestamp_ns()
    local ts = wrk.time()
    -- 返回纳秒时间戳（作为数字）
    -- 注意：lua 的数字精度有限，但对于时间戳来说通常足够
    return math.floor(ts * 1000000000)
end

-- 请求函数：为每个请求生成请求体
function request()
    -- 随机选择一个设备 ID
    local device_id = device_ids[math.random(#device_ids)]
    
    -- 生成时间戳
    local timestamp_ns = get_timestamp_ns()
    
    -- 构建 command 对象
    local command_obj = {
        cmd = "bench",
        paras = {
            timestamp = timestamp_ns
        },
        serviceId = "bench"
    }
    
    -- 构建标准格式的 payload
    local payload = {
        command = json_encode(command_obj),
        commandType = 1,
        deviceId = device_id,
        gatewayId = device_id,
        expire = 5,
        qos = 1
    }
    
    -- 如果提供了 productId，添加到 payload 中
    if product_id then
        payload.deviceProductId = product_id
        payload.gatewayProductId = product_id
    end
    
    -- 返回请求
    return wrk.format("POST", nil, nil, json_encode(payload))
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

