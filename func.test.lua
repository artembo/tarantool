os.execute("rm -rf *.snap *.xlog")

box.cfg{}

box.schema.space.create('test')
box.space.test:create_index("pk")
box.space.test:insert({3})

test = [[
        print("STARTED")
        print("FIRST YIELD")
        box.space.test:insert({1})
        box.space.test:insert({2})
        box.space.test:insert({4})
        print("FINISHED")
]]

--box.schema.func.create('test', {body = test, language = "LUA", exports = {'LUA'}, is_sandboxed = false})


box.thread.new(test)
