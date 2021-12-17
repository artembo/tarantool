#!/usr/bin/env tarantool
local test = require("sqltester")
test:plan(3)

-- Make sure that non-persistent aggregate functions are working as expected.

local step = function(x, y)
    if x == nil then
        x = {sum = 0, count = 0}
    end
    x.sum = x.sum + y
    x.count = x.count + 1
    return x
end

local finalize = function(x)
    return x.sum / x.count
end

rawset(_G, 'S1', {step = step, finalize = finalize})
local def1 = {aggregate = 'group', returns = 'number', param_list = {'integer'},
              exports = {'SQL'}}
box.schema.func.create('S1', def1);

test:execsql([[
    CREATE TABLE t(i INTEGER PRIMARY KEY AUTOINCREMENT, a INT);
    INSERT INTO t(a) VALUES(1), (1), (2), (2), (3);
]])

test:do_execsql_test(
    "gh-2579-1",
    [[
        SELECT avg(a), s1(a) FROM t;
    ]], {
        1, 1.8
    })

test:do_execsql_test(
    "gh-2579-2",
    [[
        SELECT avg(distinct a), s1(distinct a) FROM t;
    ]], {
        2, 2
    })

test:do_test(
    "gh-2579-3",
    function()
        local def2 = {aggregate = 'group', returns = 'number',
                      param_list = {'integer'}, exports = {'LUA', 'SQL'}}
        local res = {pcall(box.schema.func.create, 'S2', def2)}
        return {tostring(res[2])}
    end, {
        "Failed to create function 'S2': aggregate function can only be "..
        "accessed in SQL"
    })

test:finish_test()
