io.input("/var/run/mysql-proxy.ip")

function trim (s)
  return (string.gsub(s, "^%s*(.-)%s*$", "%1"))
end

function execute(node, query)
  os_result = os.execute('sh /scaledb/scripts/mysql-client.sh '..node..' "FLUSH TABLES;"')
end

local ip_addr = trim(io.read("*all"))

print(ip_addr..' Calling a table flush')

-- Loop through the other nodes
for node in lfs.dir("/scaledb/nodes") do
  -- Check to make sure we're not considering '.' or '..'
  if node ~= "." and node ~= ".." then
    -- If it's our current node in the loop do nothing, otherwise continue
    if node ~= ip_addr then
      --print(node)
      execute(node, "FLUSH TABLES")
      print(node..' Flushed')
    end
  end
end
