-- wrk lua 脚本 - 标准模式
-- 使用方法: wrk -s wrk_standard.lua -t 4 -c 100 -d 30s --timeout 30s -H "edgeNodeId: YOUR_EDGE_NODE_ID" http://your-url

-- 设备 ID 列表（将从 CSV 文件读取或使用默认值）
local device_ids = {}

-- CSV 文件路径（可通过命令行参数传入）
local csv_file = "devices.csv"

-- 产品 ID（可选，通过全局变量指定，可在脚本生成时设置）
local product_id = "57b0817bf15132759bccbefbdb38bf23"

-- 每个设备的请求计数器（用于 mid）
local device_counters = {}

-- 当前设备索引（已废弃，改为随机选择）
-- local current_device_index = 1

-- 打印响应结果开关（默认为关闭）
print_responses = false
-- 打印响应计数器（用于限制打印数量，避免输出过多）
print_response_count = 0
max_print_responses = 100  -- 最多打印的响应数量（0 表示不限制）

-- 统计变量（每个线程独立）
success_count = 0
http_error_count = 0
business_error_count = 0
parse_error_count = 0

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

-- 初始化函数：在每个线程启动时执行
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
            local error_msg = "device_ids 列表为空，请确保 CSV 文件存在或使用 generate_wrk_script.py 生成脚本"
            if err then
                error_msg = error_msg .. " (" .. err .. ")"
            end
            error(error_msg)
        end
    end
    
    -- 初始化每个设备的计数器
    for i, device_id in ipairs(device_ids) do
        device_counters[device_id] = 0
    end
    
    -- 初始化统计计数器
    success_count = 0
    http_error_count = 0
    business_error_count = 0
    parse_error_count = 0
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
    
    -- 随机选择一个设备 ID
    local random_index = math.random(1, #device_ids)
    local device_id = device_ids[random_index]
    
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
    
    -- 构建 command 对象
    local command_obj = {
        cmd = "bench",
        deviceId = device_id,
        eventTime = event_time,
        expire = 1,
        mid = mid,
        msgId = msg_id,
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
        expire = 1,
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


-- JSON 解码函数（简化版，用于解析 JSON 字符串）
-- 仅用于提取响应体中的 code、success、message 等字段
function json_decode(str)
    if not str or str == "" then
        return nil
    end
    
    -- 移除首尾空白
    str = string.match(str, "^%s*(.-)%s*$")
    
    -- 检查是否是 JSON 对象
    if not (string.sub(str, 1, 1) == "{" and string.sub(str, -1, -1) == "}") then
        return nil
    end
    
    local obj = {}
    
    -- 提取 "code": 数字
    local code = string.match(str, '"code"%s*:%s*(%d+)')
    if code then
        obj.code = tonumber(code)
    end
    
    -- 提取 "success": true/false
    local success = string.match(str, '"success"%s*:%s*(true)')
    if success then
        obj.success = true
    else
        local success_false = string.match(str, '"success"%s*:%s*(false)')
        if success_false then
            obj.success = false
        end
    end
    
    -- 提取 "message": "..."
    local message = string.match(str, '"message"%s*:%s*"([^"]*)"')
    if message then
        obj.message = message
    end
    
    return obj
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
    -- 判断是否成功
    local is_success = false
    local error_type = ""
    local error_message = ""
    
    -- 首先检查 HTTP 状态码
    if status < 200 or status >= 400 then
        -- HTTP 错误
        http_error_count = http_error_count + 1
        error_type = "HTTP错误"
        error_message = string.format("status=%d", status)
        is_success = false
    else
        -- HTTP 状态码正常，检查响应体中的业务错误
        if body and body ~= "" then
            -- 尝试解析 JSON
            local parsed = json_decode(body)
            if parsed then
                -- 检查 code 字段
                if parsed.code and tonumber(parsed.code) ~= 0 then
                    -- 业务错误：code 非 0
                    business_error_count = business_error_count + 1
                    error_type = "业务错误"
                    error_message = string.format("code=%s", tostring(parsed.code))
                    if parsed.message then
                        error_message = error_message .. string.format(", message: %s", parsed.message)
                    end
                    is_success = false
                elseif parsed.success == false then
                    -- 业务错误：success 为 false
                    business_error_count = business_error_count + 1
                    error_type = "业务错误"
                    error_message = "success=false"
                    if parsed.code then
                        error_message = string.format("code=%s, ", tostring(parsed.code)) .. error_message
                    end
                    if parsed.message then
                        error_message = error_message .. string.format(", message: %s", parsed.message)
                    end
                    is_success = false
                else
                    -- 业务成功
                    success_count = success_count + 1
                    is_success = true
                end
            else
                -- JSON 解析失败
                parse_error_count = parse_error_count + 1
                error_type = "解析错误"
                error_message = "invalid_json"
                is_success = false
            end
        else
            -- 没有响应体，但 HTTP 状态码正常，视为成功
            success_count = success_count + 1
            is_success = true
        end
    end
    
    -- 为失败请求输出简单标记（用于统计）
    if not is_success then
        io.write(string.format("[FAIL:%s]\n", error_type))
    end
    
    -- 如果开关打开，打印详细响应信息
    if print_responses ~= false then
        -- 确保计数器已初始化
        if print_response_count == nil then
            print_response_count = 0
        end
        if max_print_responses == nil then
            max_print_responses = 100
        end
        
        -- 检查是否超过最大打印数量（0 表示不限制）
        if max_print_responses == 0 or print_response_count < max_print_responses then
            print_response_count = print_response_count + 1
            local status_icon = is_success and "✓" or "✗"
            local status_label = is_success and "成功" or "失败"
            
            io.write(string.format("[响应 %d] %s 状态码: %d (%s)\n", print_response_count, status_icon, status, status_label))
            
            if not is_success then
                io.write(string.format("错误类型: %s\n", error_type))
                io.write(string.format("错误信息: %s\n", error_message))
            end
            
            if body and body ~= "" then
                io.write("响应体: " .. body .. "\n")
            else
                io.write("响应体: (无响应体)\n")
            end
            
            -- 显示当前线程统计
            local total = success_count + http_error_count + business_error_count + parse_error_count
            io.write(string.format("[线程统计] 总数=%d (成功=%d, HTTP错误=%d, 业务错误=%d, 解析错误=%d)\n",
                total, success_count, http_error_count, business_error_count, parse_error_count))
            
            io.write("-" .. string.rep("-", 70) .. "\n")
        end
    end
end

-- 测试结束函数
function done(summary, latency, requests)
    io.write("\n"..string.rep("=", 80).."\n")
    io.write("测试统计摘要\n")
    io.write(string.rep("=", 80).."\n\n")
    
    -- HTTP 层统计
    io.write("HTTP 层统计:\n")
    io.write(string.format("  总请求数: %d\n", summary.requests))
    io.write(string.format("  总时长: %.2f 秒\n", summary.duration / 1000000))
    io.write(string.format("  平均 QPS: %.2f req/s\n", summary.requests / (summary.duration / 1000000)))
    io.write(string.format("  平均延迟: %.2f ms\n", latency.mean / 1000))
    io.write(string.format("  最大延迟: %.2f ms\n", latency.max / 1000))
    
    io.write("\n"..string.rep("-", 80).."\n")
    io.write("业务层统计 (基于响应内容分析):\n")
    io.write(string.rep("-", 80).."\n\n")
    
    io.write("上方输出中每个失败请求都有一行 [FAIL:错误类型] 标记\n\n")
    
    io.write("统计命令示例 (将输出保存到文件后使用):\n")
    io.write("  # 保存输出: wrk ... 2>&1 | tee output.log\n\n")
    io.write("  总失败数:       grep -c '\\[FAIL:' output.log\n")
    io.write("  HTTP错误数:     grep -c '\\[FAIL:HTTP错误\\]' output.log\n")
    io.write("  业务错误数:     grep -c '\\[FAIL:业务错误\\]' output.log\n")
    io.write("  解析错误数:     grep -c '\\[FAIL:解析错误\\]' output.log\n\n")
    
    io.write("计算成功率:\n")
    io.write(string.format("  总请求数 = %d\n", summary.requests))
    io.write("  失败数 = grep -c '\\[FAIL:' output.log\n")
    io.write("  成功数 = 总请求数 - 失败数\n")
    io.write("  成功率 = 成功数 / 总请求数 * 100%\n\n")
    
    io.write("一键计算成功率脚本:\n")
    io.write("  TOTAL="..tostring(summary.requests).."; ")
    io.write("FAIL=$(grep -c '\\[FAIL:' output.log 2>/dev/null || echo 0); ")
    io.write("SUCCESS=$((TOTAL - FAIL)); ")
    io.write("echo \"成功: $SUCCESS/$TOTAL ($(awk \"BEGIN{printf \\\"%.2f\\\", $SUCCESS/$TOTAL*100}\")%)\"\n")
    
    io.write("\n"..string.rep("=", 80).."\n\n")
end

