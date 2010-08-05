
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
require 'openssl'

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
  
  def reap_proxies
    @proxies.each do |key, proxy|
      terminate_remote(proxy) if proxy and proxy.old?
    end
  end

  def send_client_connect(index)
    puts "Asking client to connect to #{index}"  if VERBOSE
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
        if VERBOSE
          puts "#{@port}: #{@available_proxies.length} proxies available"
          puts "#{@port}: All: #{@proxies.values.join(',' )}"
          puts "#{@port}: Available: #{@available_proxies.join(',' )}"
        end
        proxy = @available_proxies.pop
      end
      if proxy
        send_client_connect(proxy.index)
      else
        # Recycle open proxy indexes
        index, = @proxies.find { |k, v| v.nil? }
        index = @index += 1 unless index
        if index > 50
          raise LeakError, "More than 50 proxies, shutting down"
        end
        source = nil
        Timeout::timeout(30) do
          send_client_connect(index)
          source = @@socket_queue.pop
        end
        proxy = @proxies[index] = Proxy.new(source, self, index)
      end

      puts "#{@port}: Connecting proxy for #{proxy.index}" if VERBOSE

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
        puts "#{@port}: (#{proxy.index}) Adding to available pool" if VERBOSE
        @available_proxies << proxy
      else
        if proxy.dead
          puts "#{@port}: (#{proxy.index}) Proxy is dead, remove it from list" if VERBOSE
          @proxies[proxy.index] = nil
        else
          puts "#{@port}: (#{proxy.index}) Source ready? #{proxy.source_ready}" if VERBOSE
        end
      end
    end
  end
  
  def terminate_remote(proxy)
    @mutex.synchronize do
      puts "#{@port}: Sending shutdown for #{proxy.index}" if VERBOSE
      @command.write("T%06d\n" % proxy.index)
      @command.flush
    
      # Removing from active list and pool
      @proxies[proxy.index] = nil
      @available_proxies.delete(proxy)
    end
  end
  
  def send_heartbeat
    @command.write("PING   \n")
    @command.flush
    @last_heartbeat = Time.now
  end

  def run
    @cmd_thread = Thread.new do
      begin
        last_received = Time.now
        send_heartbeat
        while @active
          # Heartbeat the command socket every 10 seconds
          socks = IO.select([@command], nil, nil, 10.0)
          unless socks
            send_heartbeat
            reap_proxies
            
            # Check if client has not sent a message for 30 seconds
            # Shutdown the connection in this case
            if Time.now - last_received > 30.0
              puts "We have not received a command in 30 seconds"
              shutdown
              break
            else
              next
            end
          end
          cmd = @command.read(8)
          if cmd.nil? or cmd.length == 0
            shutdown
            break
          end
          
          last_received = Time.now

          puts "#{@port}: Received command #{cmd}" if VERBOSE
          oper, ind = cmd[0], cmd[1..-1].to_i
          case oper
          when ?S
            if VERBOSE
              puts "#{@port}: returing #{ind} to available pool"  
              puts "#{@port}: #{@proxies.values.join(',' )}"
            end
            proxy = @proxies[ind]
            if proxy
              @mutex.synchronize do
                if proxy.dest.nil? and !proxy.dead
                  @available_proxies << proxy unless @available_proxies.include?(proxy)
                  puts "#{@port}: #{@available_proxies.length} proxies now available" if VERBOSE
                else
                  proxy.source_ready = true
                end
              end
            else
              puts "#{@port}: Could not find proxy: #{ind}"
            end
            
          when ?T
            puts "Terminate request on connection #{ind}" if VERBOSE
            if !(proxy = @proxies[ind]).nil?
              proxy.shutdown
            else
              puts "Terminate: Cannot find proxy for #{ind}"
            end          
            
          when ?P
            puts "Received a pong" if VERBOSE
            
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

    puts " #{@port}: shutting down"
    @command.sysclose rescue puts "Command: #{$!}"
    @proxy.close rescue puts "Proxy: #{$!}"
    @proxies.values.each { |p| p.shutdown if p }

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
    Socket.do_not_reverse_lookup = true
    @ports = ports
    @ssl_ctx = configure_ssl
    @servers = ports.map { |port| 
      OpenSSL::SSL::SSLServer.new(TCPServer.new(port), @ssl_ctx) 
    }
    @servers.each { |server| server.start_immediately = true }
    @servers.each { |s| s.listen(10) }
    @clients = Hash.new
  end
  
  def configure_ssl
    OpenSSL.debug = true
    ca_cert  = OpenSSL::X509::Certificate.new(File.read("CA/cacert.pem"))
    ssl_cert = OpenSSL::X509::Certificate.new(File.read("server/cert_server.pem"))
    ssl_key  = OpenSSL::PKey::RSA.new(File.read("server/server_keypair.pem"))

    ctx = OpenSSL::SSL::SSLContext.new
    ctx.cert = ssl_cert
    ctx.key = ssl_key
    ctx.client_ca = ca_cert
    ctx.ca_file = "CA/cacert.pem"
    ctx.verify_mode = OpenSSL::SSL::VERIFY_PEER | OpenSSL::SSL::VERIFY_FAIL_IF_NO_PEER_CERT
    
    ctx
  end

  def accept
    while true
      puts "Accepting... #{@ports.join(', ')}" if VERBOSE
      servers, = IO.select(@servers)
      next unless servers
      servers.each do |server|
        s = nil
        begin
          s = server.accept 
          if FILTER[s.peeraddr[3]]
            puts "**** Filter out connection request from #{s.peeraddr[3]}"
            s.close
            next
          end
        rescue Errno::EAGAIN, Errno::ECONNABORTED, Errno::EPROTO, Errno::EINTR
          puts "Accept was blocked, skip for now." if VERBOSE
          next
        rescue OpenSSL::SSL::SSLError
          puts "SSL Authentication failed #{$!}"
          next
        end
        cmd = ''
        begin
          while cmd.length < 8 
            cmd << s.sysread(8 - cmd.length)
            puts "CMD: #{cmd}" if VERBOSE
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

        puts "Received command #{cmd}" if VERBOSE
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
    puts "Removing client #{client.port}" if VERBOSE
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


