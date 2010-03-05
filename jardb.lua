require('lfs')

local db = {}

function trim(s)
  return (string.gsub(s, "^%s*(.-)%s*$", "%1"))
end

function explode(div,str)
  if (div=='') then return false end
  local pos,arr = 0,{}
  -- for each divider found
  for st,sp in function() return string.find(str,div,pos,true) end do
    table.insert(arr,string.sub(str,pos,st-1)) -- Attach chars left of current divider
    pos = sp + 1 -- Jump past current divider
  end
  table.insert(arr,string.sub(str,pos)) -- Attach chars right of last divider
  return arr
end

function sleep(n)
  os.execute("sleep " .. tonumber(n))
end

function connect(node)
  local handle = {}
  local luasql = require("luasql.mysql")
  handle.env = assert(luasql.mysql())

  handle.con, err = handle.env:connect('test','axis-user','QEphA28a',node,3306)

  if handle.con == nil then
    return false, err
  else
    return handle
  end
end

function execute(node,query)
  if db.con == nil then
    db = connect(node)
  end
  db.cur, err = db.con:execute(query)
  if db.cur == nil then
    db.con:close()
    db.env:close()
    return false, err.." QUERY: "..query
  elseif type(db.cur) == "number" then
    return db
  else
    db.row, err = db.cur:fetch({}, "a")
    if db.row == nil then
      db.cur:close()
      db.con:close()
      db.env:close()
      return false, err
    else
      return db
    end
  end
end

function disconnect()
  db.con:close()
  db.env:close()
  db.con = nil
  db.env = nil
end

function doquery(query)
  io.input("/var/run/mysql-proxy.ip")
  local ip_addr = trim(io.read("*all"))

  print(ip_addr..' Calling "'..query..'"')
  for node in lfs.dir("/scaledb/nodes") do
    -- Check to make sure we're not considering '.' or '..'
    if node ~= "." and node ~= ".." then
      -- If it's our current node in the loop do nothing, otherwise continue
      if node ~= ip_addr then
        result, err = execute(node, query)
        if err then
          print(err)
        else
          print(node..' Query Successful')
        end
      end
    end
  end
end

function read_query(packet)
  if string.byte(packet) == proxy.COM_QUERY then
    local query = string.sub(packet, 2)
    print ("Received "..query)
    if string.match(string.upper(query), '^%s*DROP TABLE') or string.match(string.upper(query), '^%s*DROP DATABASE')  or string.match(string.upper(query), '^%s*CREATE') then
      -- First set up our normal query to run
      proxy.queries:append(1, string.char(proxy.COM_QUERY)..query)
      -- Now run flush across all of the other nodes
      doquery("FLUSH TABLES")
      disconnect()
      -- Return our initial query
      return proxy.PROXY_SEND_QUERY
    end
    if string.match(string.upper(query), '^%s*INSERT') then
      -- Lock the table associated with this query on other nodes
      shrapnel = explode(' ',query)
      next = false
      for k,v in pairs(shrapnel) do
        if next == true then
          tbl_name = v
          break
        end
        if string.upper(v) == "INTO" then
          next = true
        end
      end 
      doquery("LOCK TABLES "..tbl_name.." READ")
      -- Set up our normal query to run
      proxy.queries:append(1, string.char(proxy.COM_QUERY)..query)
      doquery("UNLOCK TABLES")
      disconnect()
      --sleep(3) 
      -- Now run flush across all of the other nodes
      doquery("FLUSH TABLES")
      disconnect()
      -- Return our initial query
      return proxy.PROXY_SEND_QUERY
    end
  end
end
