
require 'socket'
require 'proxy'

if ARGV.length < 5
  puts "Usage: proxy_client <remote_host> <remote_port> <remote_url_port> <local_host> <local_port> "
  exit 999
end

class ProxyClient
  def initialize(server, port, remote_port, local_host, local_port)
    @server, @port, @remote_port, @local, @local_port =
      server, port, remote_port, local_host, local_port

    @proxies = Hash.new
  end

  def run
    while true
      begin
        puts "Opening connect to server #{@server} on #{@port}"
        @command = TCPSocket.new(@server, @port)
        @command.write("C%06d\n" % @remote_port)
      rescue
        puts $!
        puts "Waiting 10 seconds and retrying"
        sleep 10
        retry
      end
      
      puts "Tunnel created, waiting for requests"
    
      while cmd = @command.read(8)
        puts "Received command #{cmd}"
        
        dest = source = proxy = nil
        oper, ind = cmd[0], cmd[1..-1].to_i
        case oper
        when ?C
          begin
            if (proxy = @proxies[ind]).nil?
              puts "Proxying(#{ind}) from #{@local}:#{@local_port} to #{@server}:#{@port}"
              source = TCPSocket.new(@server, @port)
              source.write("P%06d\n" % @remote_port)
              
              proxy = @proxies[ind] = Proxy.new(source, self, ind)
              dest = TCPSocket.new(@local, @local_port)
            else
              puts "Recycling old proxy: #{ind}"
              dest = TCPSocket.new(@local, @local_port)
            end
            
            proxy.dest = dest

          rescue Errno::ECONNREFUSED
            proxy.send_terminator

          rescue
            begin
              source.shutdown if source
              dest.shutdown if dest
            rescue
            end
            puts $!, $!.class
            puts $!.backtrace.join("\n")
          end
          
        when ?S
          proxy = @proxies[ind]
          # proxy.shutdown_dest if proxy

        when ?F
          puts "Could not bind to port #{@remote_port}, already in use - try another port"
          puts "Exiting..."
          @command.shutdown rescue
          exit(999)
          
        else
          puts "Received bad command: #{cmd}"
          
          # Try to recover
          while @command.read(1) != "\n"; end
        end
      end

      @proxies.values.each { |proxy| proxy.shutdown }
      @proxies.clear
      
      puts "Server disconnected... waiting 10 seconds and try to connect"
      sleep 10
    end
  end

  def shutdown_remote(proxy)
  end
end

client = ProxyClient.new(ARGV[0], ARGV[1].to_i,
                         ARGV[2].to_i,
                         ARGV[3], ARGV[4].to_i)
client.run
