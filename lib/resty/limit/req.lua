-- Copyright (C) 2014 Monkey Zhang (timebug), UPYUN Inc.


local floor = math.floor
local tonumber = tonumber
local cjson = require("cjson")
local redis = nil
if not package.loaded['redis'] then
    local ok
    ok, redis = pcall(require, "resty.redis")
    if not ok then
        ngx.log(ngx.ERR, "failed to require resty.redis")
        return _M.OK
    end
    redis.add_commands("ping")
end

local _M = { _VERSION = "0.02", OK = 1, BUSY = 2, FORBIDDEN = 3 }

local log_level = ngx.INFO

local function _redis_connect(rds)
    rds.host = rds.host or "127.0.0.1"
    rds.port = rds.port or 6379
    rds.timeout = rds.timeout or 1

    local red = redis:new()

    red:set_timeout(rds.timeout * 1000)

    local r, err
    if rds.socket then
        _, err = red:connect(rds.socket)
        r = rds.socket
    else
        _, err = red:connect(rds.host, rds.port)
        r  = rds.host
    end

    if err then
        ngx.log(log_level, "failed to connect to redis host: " .. r .. " ", err)
        return
    end

    if rds.pass then
        local _, err = red:auth(rds.pass)
        if err then
            ngx.log(log_level, "failed to authenticate to redis host" .. r .. " ", err)
            return
        end
    end

    return red
end


local function _redis_keepalive(conn, timeout, size)
    local _, err = conn:set_keepalive(timeout, size)
    if err then
        ngx.log(log_level, "failed to set keepalive: ", err)
    end
end


local function _redis_lookup(redis_conn, zone, key, rate, interval, burst)
    local excess, last, forbidden = 0, 0, 0

    local res, err = redis_conn:get(zone .. ":" .. key)
    _redis_keepalive(redis_conn, 10000, 100)

    if not res or res == ngx.null then
        return {_M.OK, excess}
    end

    if type(res) == "table" and res.err then
        return {_M.OK, excess}, res.err
    end

    if type(res) == "string" then
        local v = cjson.decode(res)
        if v and #v > 2 then
            excess, last, forbidden = v[1], v[2], v[3]
        end

        if forbidden == 1 then
            return {_M.FORBIDDEN, excess}
        end

        local now = ngx.now() * 1000
        local ms = math.abs(now - last)
        excess = excess - rate * ms / 1000 + 1000

        if excess < 0 then
            excess = 0
        end

        if excess > burst then
            return {_M.BUSY, excess}
        end
    end

    return {_M.OK, excess}
end


local function _redis_update(redis_conn, zone, key, excess, interval, burst)
    local res, err = nil, nil
    local now = ngx.now() * 1000

    if excess > burst and interval > 0 then
        res, err = redis_conn:setex(zone .. ":" .. key, interval,
                                          cjson.encode({excess, now, 1}))
    else
        res, err = redis_conn:setex(zone .. ":" .. key, 60,
                                          cjson.encode({excess, now, 0}))
    end

    _redis_keepalive(redis_conn, 10000, 100)

    if err then
        return nil, err
    end

    if type(res) == "table" and res.err then
        return nil, res.err
    end

    return _M.OK
end


function _M.limit(cfg)
    if not package.loaded['redis'] then
        local ok, redis = pcall(require, "resty.redis")
        if not ok then
            ngx.log(log_level, "failed to require resty.redis")
            return _M.OK
        end
    end

    local zone = cfg.zone or "limit_req"
    local key = cfg.key or ngx.var.remote_addr
    local rate = cfg.rate or "1r/s"
    local burst = cfg.burst * 1000 or 0
    local interval = cfg.interval or 0
    log_level = cfg.log_level or log_level

    local conn_masters = {}
    for i, master in ipairs(cfg.redis.masters) do
        local host = master["host"]
        conn_masters[host] = _redis_connect(master)
        if not conn_masters[host] then
            ngx.log(log_level, "failed to connect to some or all configured redis masters")
            return _M.OK
        end
    end

    local conn_slave = _redis_connect(cfg.redis.slave)
    if not conn_slave then
        ngx.log(log_level, "failed to connect to redis slave")
        return _M.OK
    end

    local scale = 1
    local len = #rate

    if len > 3 and rate:sub(len - 2) == "r/s" then
        scale = 1
        rate = rate:sub(1, len - 3)
    elseif len > 3 and rate:sub(len - 2) == "r/m" then
        scale = 60
        rate = rate:sub(1, len - 3)
    end

    rate = floor((tonumber(rate) or 1) * 1000 / scale)

    local res, err = _redis_lookup(conn_slave, zone, key, rate, interval, burst)
    if res and (res[1] == _M.BUSY or res[1] == _M.FORBIDDEN) then
        if res[1] == _M.BUSY then
            ngx.log(log_level, "excess requests " ..
                    zone .. ":" .. key .. " - " .. res[2]/1000 )
        end
        return
    end

    if not res then
        ngx.log(log_level, "redis lookup error for key " .. zone .. ":" .. key)
    end

    for host, conn in pairs(conn_masters) do
        local _, err = conn:ping()
        if err then
            ngx.log(log_level, "failed to ping redis master: " .. host ..
                              " error: ", err)
            return _M.OK
        end
    end

    for host, conn in pairs(conn_masters) do
        local _, err = _redis_update(conn, zone, key, res[2], interval, burst)
        if err then
            ngx.log(log_level, "failed to update redis master: " .. host ..
                              " error: ", err)
            return _M.OK
        end
    end

    return _M.OK
end


return _M
