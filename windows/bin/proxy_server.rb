
# Set up a forwarding

# Server proxy --
# Protocol
# Client creates command socket by connecting to server. When server wants to send
# data, server send request to client with new port to connect to. The client then creates a
# outgoing connection to the server and begins to receive data forwarding it to the
# local destination. Results are then proxied back to the originator.

Dir.chdir(File.dirname(__FILE__))

require 'rubygems'
require 'socket'
require 'proxy'
require 'thread'
require 'timeout'

STDOUT.sync = true

class CommandSocket
  attr_reader :port, :remote_addr

  @@socket_queue = Queue.new
  @@shutdown_mutex = Mutex.new

  def initialize(command, server, port)
    @command = command
    @command.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, true)
    @remote_addr = @command.peeraddr[3]
    @proxies = Hash.new
    @available_proxies = Array.new
    @active = true
    @index = 0
    @port = port
    @server = server
    @mutex = Mutex.new
  end

  def same_peer?(sock)
    @remote_addr == sock.peeraddr[3]
  end

  def create_proxy(port)
    @port = port
    @proxy = TCPServer.new('127.0.0.1', port)
    true
  rescue
    puts $!
    puts "Cannot create server port"
    false
  end

  def send_client_connect(index)
    # puts "Asking client to connect to #{index}"
    unless @command.write("C%06d\n" % index)
      shutdown
      raise "Write failed, shutting down"
    end
  end

  def accept
    client = @proxy.accept

    begin
      proxy = nil
      @mutex.synchronize do
        puts "#{@port}: #{@available_proxies.length} proxies available"
        proxy = @available_proxies.pop
      end
      if proxy
        send_client_connect(proxy.index)
      else
        @index += 1
        source = nil
        Timeout::timeout(20) do
          send_client_connect(@index)
          source = @@socket_queue.pop
        end
        proxy = @proxies[@index] = Proxy.new(source, self, @index)
      end

      puts "#{@port}: Connecting proxy for #{proxy.index}"

      proxy.start
      proxy.dest = client

    rescue Proxy::DestError
      puts $!
      puts "#{@port}: Shutting down proxy and retying"
      proxy.shutdown
      @proxies[proxy.index] = nil
      retry

    rescue IOError
      puts "#{@port}: Server socket closed"
      shutdown
    end

  rescue
    puts $!, $!.class
    puts $!.backtrace.join("\n")
    shutdown
  end

  def add_socket(sock)
    @@socket_queue << sock
  end

  def shutdown_remote(proxy)
    @mutex.synchronize do
      if proxy.dest
        raise "#{@port}: Can't add proxy when dest is set!"
      end
      if proxy.source_ready and !@available_proxies.include?(proxy) and @active
        @available_proxies << proxy
      end
    end
  end

  def run
    @cmd_thread = Thread.new do
      begin
        while @active
          cmd = @command.read(8)
          if cmd.nil? or cmd.length == 0
            shutdown
            break
          end

          # puts "Received command #{cmd}"
          oper, ind = cmd[0], cmd[1..-1].to_i
          case oper
          when ?S
            puts "#{@port}: returing #{ind} to available pool"
            proxy = @proxies[ind]
            @mutex.synchronize do
              if proxy.dest.nil?
                @available_proxies << proxy unless @available_proxies.include?(proxy)
                puts "#{@port}: #{@available_proxies.length} proxies now available"
              else
                proxy.source_ready = true
              end
            end

          else
            puts "#{@port}: Received invalid command #{oper}"
          end

        end
      rescue Errno::ECONNRESET
        puts "#{@port}: Client disconnected: #{@command.addr.inspect}"
      rescue
        puts $!, $!.class, $!.backtrace.join("\n")
      end

      shutdown
      puts "#{@port}: Exiting command thread for #{@port}"
    end

    @accept = Thread.new do
      while @active
        accept
      end

      puts "#{@port}: Exiting accept thread "
    end
  end

  def shutdown
    @@shutdown_mutex.synchronize do
      return unless @active
      @active = false
    end

    @server.remove_client(self)

    puts " #{@port}: hutting down"
    @command.shutdown rescue puts "Command: #{$!}"
    @proxy.close rescue puts "Proxy: #{$!}"
    @proxies.values.each { |p| p.shutdown }

    @accept.kill unless Thread.current == @accept
    @cmd_thread.kill unless Thread.current == @cmd_thread

    puts "#{@port} is now shutdown"
  rescue
    puts $!, $!.class
    puts $!.backtrace.join("\n")
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
    puts "Waiting on #{@port}"
    while true
      s = @server.accept
      cmd = s.read(8)
      # puts "Received command #{cmd}"
      if cmd !~ /[CP]\d{6}\n/
          puts "bad connection request: #{cmd.inspect}"
      else
        oper, port = cmd[0], cmd[1..-1].to_i
        if oper == ?C
          if port.nil? or port.to_i < 1025
            puts "Received bad port: #{port}"
            next
          end

          client = @clients[port]
          if client
            if client.same_peer?(s)
              client.shutdown
              sleep 1
            else
              puts "Could not create proxy server"
              s.write("F0000001\n")
              s.shutdown
            end
          else
            puts "Creating proxy server on port #{port}"
            client = CommandSocket.new(s, self, port)
            if client.create_proxy(port.to_i)
              @clients[port] = client
              client.run
            else
              puts "Could not create proxy server for #{port}"
              s.write("F0000001\n")
              s.shutdown
            end
          end

        elsif oper == ?P
          client = @clients[port]
          if client and client.same_peer?(s)
            client.add_socket(s)
          else
            puts "Could not find client for #{port}"
          end
        end
      end
    end
  end

  def remove_client(client)
    puts "Removing client #{client.port}"
    @clients.delete(client.port)
  end
end

if ARGV[0] == '-v'
  VERBOSE = true
  ARGV.shift
else
  VERBOSE = false
end

if ARGV.length < 1
  puts "Usage: proxy_server <port>"
  exit 999
end

server = Server.new(ARGV[0].to_i)
server.accept


