
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

FILTER = {}

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
    @proxy = TCPServer.new(port)
    true
  rescue
    puts $!
    puts "Cannot create server port"
    false
  end

  def send_client_connect(index)
    puts "Asking client to connect to #{index}"
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
        if VERBOSE
          puts "#{@port}: All: #{@proxies.values.join(',' )}"
          puts "#{@port}: Available: #{@available_proxies.join(',' )}"
        end
        proxy = @available_proxies.pop
      end
      if proxy
        send_client_connect(proxy.index)
      else
        @index += 1
        if @index > 50
          raise LeakError, "More than 50 proxies, shutting down"
        end
        source = nil
        Timeout::timeout(30) do
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
    
  rescue Proxy::LeakError
    puts "#{@port}: #{$!}"
    shutdown

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
        raise "#{@port}: ********* Can't add proxy when dest is set! ********"
      end
      if proxy.source_ready and !proxy.dead and !@available_proxies.include?(proxy) and @active
        puts "#{@port}: (#{proxy.index}) Adding to available pool"
        @available_proxies << proxy
      else
        if proxy.dead
          puts "#{@port}: (#{proxy.index}) Proxy is dead, remove it from list"
          @proxies[proxy.index] = nil
        else
          # puts "#{@port}: (#{proxy.index}) Source ready? #{proxy.source_ready}"
        end
      end
    end
  end
  
  def terminate_remote(proxy)
    puts "#{@port}: Sending shutdown for #{proxy.index}"
    @command.write("T%06d\n" % proxy.index)
    @command.flush
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

          puts "#{@port}: Received command #{cmd}"
          oper, ind = cmd[0], cmd[1..-1].to_i
          case oper
          when ?S
            puts "#{@port}: returing #{ind} to available pool"
            # puts "#{@port}: #{@proxies.values.join(',' )}"
            proxy = @proxies[ind]
            if proxy
              @mutex.synchronize do
                if proxy.dest.nil? and !proxy.dead
                  @available_proxies << proxy unless @available_proxies.include?(proxy)
                  # puts "#{@port}: #{@available_proxies.length} proxies now available"
                else
                  proxy.source_ready = true
                end
              end
            else
              puts "#{@port}: Could not find proxy: #{ind}"
            end
            
          when ?T
            puts "Shutdown request on connection #{ind}"
            if !(proxy = @proxies[ind]).nil?
              proxy.shutdown_read
            else
              puts "Cannot find proxy for #{ind}"
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
    
    puts "Returning from run"
  end

  def shutdown
    @@shutdown_mutex.synchronize do
      return unless @active
      @active = false
    end

    @server.remove_client(self)

    puts " #{@port}: shutting down"
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
  def initialize(ports)
    @ports = ports
    @servers = ports.map { |port| TCPServer.new(port) }
    @servers.each { |s| s.listen(10) }
    @clients = Hash.new
  end

  def accept
    while true
      puts "Accepting... #{@ports.join(', ')}"
      servers, = IO.select(@servers)
      next unless servers
      servers.each do |server|
        s = nil
        begin
          s = server.accept_nonblock
          if FILTER[s.peeraddr[3]]
            puts "**** Filter out connection request from #{s.peeraddr[3]}"
            s.close
            next
          end
        rescue Errno::EAGAIN, Errno::ECONNABORTED, Errno::EPROTO, Errno::EINTR
          puts "Accept was blocked, skip for now."
          next
        end
        puts "Reading..."
        cmd = ''
        begin
          while cmd.length < 8 
            puts "Read nonblock"
            cmd << s.read_nonblock(8 - cmd.length)
            puts "CMD: #{cmd}"
          end
        rescue Errno::EAGAIN, Errno::EWOULDBLOCK
          puts "CMD: #{$!}"
          if IO.select([s], nil, nil, 3)  
            retry
          else
            puts "Read timed out: #{s.peeraddr.inspect}"
          end
        rescue
          puts "Accept: #{$!}"
        end

        puts "Received command #{cmd}"
        if cmd !~ /[CP]\d{6}\n/
          puts "bad connection request: #{cmd.inspect}"
          s.close
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

server = Server.new(ARGV.map { |p| p.to_i })
server.accept


