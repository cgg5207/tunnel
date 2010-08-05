
require 'socket'
require 'proxy'
require 'timeout'
require 'openssl'

STDOUT.sync = true

class ProxyClient
  def initialize(server, port, remote_port, local_host, local_port)
    @server, @port, @remote_port, @local, @local_port =
      server, port, remote_port, local_host, local_port

    @proxies = Hash.new
  end
  
  def configure_ssl
    OpenSSL.debug = true if VERBOSE
    ssl_cert = OpenSSL::X509::Certificate.new(File.read("client/cert_client.pem"))
    ssl_key  = OpenSSL::PKey::RSA.new(File.read("client/client_keypair.pem"))

    ctx = OpenSSL::SSL::SSLContext.new
    ctx.cert = ssl_cert
    ctx.key = ssl_key
    ctx.ca_file = "CA/cacert.pem"
    ctx.verify_mode = OpenSSL::SSL::VERIFY_PEER
    
    ctx
  end

  def run
    @ssl_ctx = configure_ssl
    
    while true
      begin
        Timeout::timeout(10) do
          puts "Opening connect to server #{@server} on #{@port}"
          @socket = TCPSocket.new(@server, @port)
        end
        @socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, true)  
        @command = OpenSSL::SSL::SSLSocket.new(@socket, @ssl_ctx)
        @command.sync_close = true        
        @command.connect
          
        @command.write("C%06d\n" % @remote_port)
      rescue
        puts $!
        puts "Waiting 10 seconds and retrying"
        sleep 10
        retry
      end
      
      puts "Tunnel created, waiting for requests"
      begin
        while true
          socks = IO.select([@command], nil, nil, 30.0)
          unless socks
            # If we have not received a command (hearbeat) in 30 seconds
            # the server is unresponsive. Shutdown and try reconnecting.
            raise "Server not communicating for 30 seconds"
          end
          
          cmd = @command.read(8)
          break unless cmd
          puts "Received command #{cmd}" if VERBOSE
        
          dest = source = proxy = nil
          oper, ind = cmd[0], cmd[1..-1].to_i
          case oper
          when ?C
            begin
              if (proxy = @proxies[ind]).nil?
                puts "Proxying(#{ind}) from #{@local}:#{@local_port} to #{@server}:#{@port}"
                source = OpenSSL::SSL::SSLSocket.new(TCPSocket.new(@server, @port), @ssl_ctx)
                source.sync_close = true        
                source.connect                
                source.write("P%06d\n" % @remote_port)
              
                proxy = @proxies[ind] = Proxy.new(source, self, ind)
                dest = TCPSocket.new(@local, @local_port)
              else
                puts "Recycling old proxy: #{ind}" if VERBOSE
                dest = TCPSocket.new(@local, @local_port)
              end

              proxy.dest = dest

            rescue Errno::ECONNREFUSED
              puts "Connection refused, sending terminator"
              proxy.send_terminator(true)
              @command.write("S%06d\n" % proxy.index)
              proxy.pull_thread

            rescue Proxy::DestError
              puts $!
              puts "Shutting down proxy and retying"
              proxy.shutdown
              @proxies[ind] = nil
              retry

            rescue
              begin
                source.shutdown if source
                dest.shutdown if dest
              rescue
              end
              puts $!, $!.class
              puts $!.backtrace.join("\n")
            end
          
          when ?F
            puts "Could not bind to port #{@remote_port}, already in use - try another port"
            puts "Exiting..."
            @command.shutdown rescue
            exit(999)

          when ?T
            puts "Terminate request on connection #{ind}"  if VERBOSE
            if !(proxy = @proxies[ind]).nil?
              proxy.shutdown
              @proxies[proxy.index] = nil
            else
              puts "Cannot find proxy for #{ind}"
            end
            
          when ?P
            puts "Received a PING" if VERBOSE
            send_heartbeat
            
          else
            puts "Received bad command: #{cmd}"
          
            # Try to recover
            begin
              @command.sysread(1000)
            rescue Errno::EAGAIN, Errno::EWOULDBLOCK
            end
          end
        end
      rescue 
        puts "Error: #{$!}"
        puts "Disconnected"
        begin
          @command.close
        rescue
        end
      end

      @proxies.values.each { |proxy| proxy.shutdown if proxy }
      @proxies.clear
      
      puts "Server disconnected... waiting 10 seconds and try to connect"
      sleep 10
    end
  end
  
  def send_heartbeat
    @command.write("PONG   \n")
    @last_heartbeat = Time.now
  end

  def shutdown_remote(proxy)
    # puts "#{proxy.index}: Sending proxy disconnect"
    @command.write("S%06d\n" % proxy.index)
    @command.flush
  end
  
  def terminate_remote(proxy) 
    puts "#{@port}: Sending shutdown for #{proxy.index}"  if VERBOSE
    @command.write("T%06d\n" % proxy.index)
    @command.flush
  end
end

if ARGV[0] == '-v'
  VERBOSE = true
  ARGV.shift
else
  VERBOSE = false
end

if ARGV.length < 5
  puts "Usage: proxy_client <remote_host> <remote_port> <remote_url_port> <local_host> <local_port> "
  exit 999
end

client = ProxyClient.new(ARGV[0], ARGV[1].to_i,
                         ARGV[2].to_i,
                         ARGV[3], ARGV[4].to_i)
client.run
