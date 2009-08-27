
# Set up a forwarding

# Server proxy --
# Protocol
# Client creates command socket by connecting to server. When server wants to send
# data, server send request to client with new port to connect to. The client then creates a
# outgoing connection to the server and begins to receive data forwarding it to the
# local destination. Results are then proxied back to the originator.

require 'socket'
require 'proxy'
require 'thread'

if ARGV.length < 1
  puts "Usage: proxy_server <port>"
  exit 999
end

class CommandSocket
  @@next_port = 9000
  @@mutex = Mutex.new
  
  def initialize(command)
    @command = command
    @proxies = Hash.new
    @active = true
  end
  
  def create_proxy(port)
    @port = port
    @proxy = TCPServer.new(port)
  end

  def accept
    client = @proxy.accept

    port = nil
    @@mutex.synchronize do 
      port = @@next_port
      @@next_port = 9000 + (@@next_port - 8999) % 1000
    end
    server = TCPServer.new(port)
    
    puts "Asking client to connect to #{port}"
    unless @command.write("C%06d\n" % port)
      puts "Write failed, shutting down"
      shutdown
      return
    end
    
    dest = server.accept
    puts "Proxying #{client.addr.inspect} to #{dest.addr.inspect}"
    @proxies[port] = Proxy.new(client, dest)

  rescue
    puts $!
    puts $!.backtrace.join

    shutdown
  end

  def handle
    Thread.new do
      while @active
        begin
          b = @command.read(1)
          shutdown if b.length.nil?
        rescue
          shutdown
        end
      end
    end
    
    Thread.new do
      while @active
        accept
      end
    end
  end

  def shutdown
    puts "Shutting down #{@command.addr.inspect} for port #{@port}"
    
    @active = false
    @proxy.shutdown
    @proxies.values.each { |p| p.shutdown }
  end
end

class Server
  def initialize(port)
    @port = port
    @server = TCPServer.new(port)
    @server.listen(10)
    @clients = Hash.new
  end
  
  def accept
    while true
      puts "Waiting on #{@port}"
      s = @server.accept
      cmd = CommandSocket.new(s)
      port = s.read(8)
      if port !~ /\d{7,7}\n/
        puts "bad connection requet: #{port.inspect}"
      else
        if port.nil? or port.to_i < 1025
          puts "Received bad port: #{port}"
          next
        end

        puts "Creating proxy server on port #{port.to_i}"
        cmd.create_proxy(port.to_i)
        cmd.handle
      end
    end
  end
end

server = Server.new(ARGV[0].to_i)
server.accept


