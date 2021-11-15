#!/usr/bin/env tarantool

local tap = require('tap')
local popen = require('popen')
local process_timeout = require('process_timeout')
local fio = require('fio')
local clock = require('clock')

--
-- gh-2717: tarantool console quit on sigint
--
local file_read_timeout = 3.0
local file_read_interval = 0.2
local file_open_timeout = 3.0

local TARANTOOL_PATH = arg[-1]
local prompt = 'tarantool> '
local test = tap.test('gh-2717')

test:plan(3)

local ph = popen.shell(TARANTOOL_PATH .. ' -i', 'r')
assert(ph.pid, 'popen error while executing ' .. TARANTOOL_PATH)

local start_time = clock.monotonic()
local deadline = 10.0

local output = ph:read()
while output:find('tarantool>') == nil and clock.monotonic() - start_time < deadline do
    output = output .. ph:read()
end

ph:signal(popen.signal.SIGINT)

start_time = clock.monotonic()
while output:find('C\ntarantool>') == nil and clock.monotonic() - start_time < deadline do
    output = output .. ph:read()
end
output = output .. ph:read()

test:like(ph:info().status.state, popen.state.ALIVE, 'SIGINT doesn\'t kill tarantool in interactive mode ')
test:like(output, prompt .. '^C\n' .. prompt, 'Ctrl+C discards the input and calls the new prompt')
ph:close()

--
-- gh-2717: testing daemonized tarantool on signaling INT
--
local log_file = fio.abspath('tarantool.log')
local pid_file = fio.abspath('tarantool.pid')
local xlog_file = fio.abspath('00000000000000000000.xlog')
local snap_file = fio.abspath('00000000000000000000.snap')
local arg = ' -e \"box.cfg{pid_file=\'' .. pid_file .. '\', log=\'' .. log_file .. '\', listen=3313}\"'

os.remove(log_file)
os.remove(pid_file)
os.remove(xlog_file)
os.remove(snap_file)

ph = popen.shell(TARANTOOL_PATH .. arg, 'r')

local f = process_timeout.open_with_timeout(log_file, file_open_timeout)
assert(f, 'error while opening ' .. log_file)

ph:signal(popen.signal.SIGINT)

local status = ph:wait(nil, popen.signal.SIGINT)
test:unlike(status.state, popen.state.ALIVE, 'SIGINT terminates tarantool in daemon mode ')

start_time = clock.monotonic()
local data = ''
while data:match('got signal 2') == nil and clock.monotonic() - start_time < deadline do
    data = process_timeout.read_with_timeout(f,
            file_read_timeout,
            file_read_interval)
end
assert(data:match('got signal 2'), 'there is no one note about SIGINT')

f:close()
ph:close()
os.remove(log_file)
os.remove(pid_file)
os.remove(xlog_file)
os.remove(snap_file)
os.exit(test:check() and 0 or 1)
