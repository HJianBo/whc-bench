-- wrk lua 脚本 - EMQX 模式
-- 使用方法: wrk -s wrk_emqx.lua -t 4 -c 100 -d 30s --timeout 30s -H "Authorization: Basic YOUR_SK" http://your-url

-- 设备 ID 列表（将从 CSV 文件读取或使用默认值）
local device_ids = {}

-- CSV 文件路径（可通过命令行参数传入）
local csv_file = "devices.csv"

-- 每个设备的请求计数器（用于 mid）
local device_counters = {}

-- 当前设备索引（用于按顺序轮流选择设备）
local current_device_index = 1

-- 打印响应结果开关（默认为打开）
print_responses = false
-- 打印响应计数器（用于限制打印数量，避免输出过多）
print_response_count = 0
max_print_responses = 100  -- 最多打印的响应数量（0 表示不限制）

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
    
    -- 初始化当前设备索引
    current_device_index = 1
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

-- 使用 LuaJIT FFI 获取高精度时间戳
-- wrk 使用 LuaJIT，支持 FFI 调用系统函数
local ffi = require("ffi")
local gettimeofday_available = false
local clock_gettime_available = false

-- 尝试初始化 gettimeofday (微秒级精度)
pcall(function()
    ffi.cdef[[
        typedef long time_t;
        typedef struct timeval {
            time_t tv_sec;
            long tv_usec;
        } timeval;
        int gettimeofday(struct timeval *tv, void *tz);
    ]]
    gettimeofday_available = true
end)

-- 尝试初始化 clock_gettime (纳秒级精度，Linux/macOS)
pcall(function()
    ffi.cdef[[
        typedef long time_t;
        typedef long clockid_t;
        typedef struct timespec {
            time_t tv_sec;
            long tv_nsec;
        } timespec;
        int clock_gettime(clockid_t clk_id, struct timespec *tp);
    ]]
    clock_gettime_available = true
end)

-- 获取当前时间戳（秒，带小数部分，高精度）
-- 优先使用 clock_gettime (纳秒级精度)，其次 gettimeofday (微秒级精度)，最后 os.time() (秒级精度)
local function get_current_time()
    -- 优先使用 clock_gettime (纳秒级精度)
    if clock_gettime_available then
        local ts = ffi.new("timespec")
        -- CLOCK_REALTIME = 0
        if ffi.C.clock_gettime(0, ts) == 0 then
            return tonumber(ts.tv_sec) + tonumber(ts.tv_nsec) / 1000000000
        end
    end
    
    -- 其次使用 gettimeofday (微秒级精度)
    if gettimeofday_available then
        local tv = ffi.new("timeval")
        if ffi.C.gettimeofday(tv, nil) == 0 then
            return tonumber(tv.tv_sec) + tonumber(tv.tv_usec) / 1000000
        end
    end
    
    -- 后备方案：使用 os.time()，但只有秒级精度
    return os.time()
end

-- 生成纳秒级时间戳（高精度）
-- 使用 FFI 调用系统函数获取高精度时间戳
-- clock_gettime 提供纳秒级精度，gettimeofday 提供微秒级精度
function get_timestamp_ns()
    -- 使用 get_current_time() 获取高精度时间戳（包含小数部分）
    -- get_current_time() 会优先使用 clock_gettime (纳秒级)，其次 gettimeofday (微秒级)
    local ts = get_current_time()
    -- 乘以 1e9 转换为纳秒时间戳
    -- 例如：1765893013.123456789 秒 -> 1765893013123456789 纳秒
    -- 使用 math.floor 确保返回整数纳秒时间戳
    -- 注意：Lua 使用双精度浮点数，对于纳秒时间戳（约19位）可能会有精度损失
    -- 但使用 clock_gettime 时可以获得纳秒级精度
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
    
    -- 按顺序轮流选择一个设备 ID
    local device_id = device_ids[current_device_index]
    current_device_index = current_device_index + 1
    if current_device_index > #device_ids then
        current_device_index = 1
    end
    
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
    -- 确保 print_responses 开关已初始化
    if print_responses == nil then
        print_responses = true
    end
    if print_response_count == nil then
        print_response_count = 0
    end
    if max_print_responses == nil then
        max_print_responses = 100
    end
    
    -- 如果开关打开，打印响应结果
    if print_responses ~= false then
        -- 检查是否超过最大打印数量（0 表示不限制）
        if max_print_responses == 0 or print_response_count < max_print_responses then
            print_response_count = print_response_count + 1
            local status_icon = (status >= 200 and status < 400) and "✓" or "✗"
            local status_label = (status >= 200 and status < 400) and "成功" or "失败"
            io.write(string.format("[响应 %d] %s 状态码: %d (%s)\n", print_response_count, status_icon, status, status_label))
            if body and body ~= "" then
                io.write("响应体: " .. body .. "\n")
            else
                io.write("响应体: (无响应体)\n")
            end
            io.write("-" .. string.rep("-", 70) .. "\n")
        end
    end
end

-- 测试结束函数（已简化，不打印统计信息）
function done(summary, latency, requests)
    -- 不打印任何统计信息，响应已在 response() 函数中实时打印
end

