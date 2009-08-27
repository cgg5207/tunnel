
require 'socket'
require 'proxy'

if ARGV.length < 5
  puts "Usage: proxy_client <remote_host> <remote_port> <remote_url_port> <local_host> <local_port> "
  exit 999
end

class ProxyClient
  def initialize(host, port, remote_port, local_host, local_port)
    @server, @port, @remote_port, @local, @local_port =
      host, port, remote_port, local_host, local_port
    
    @command = TCPSocket.new(@host, @port)
    @command.write("C%06d\n" % @remote_port)

    @proxy = Hash.new
  end

  def run
    while cmd = command.read(8)
      if cmd =~ /[CS]\d+\n/
        puts "Received command #{cmd}"
        
        oper, port = cmd[0], cmd[1..-1].to_i
        if oper == ?C
          begin
            puts "Proxying from #{@local}:#{local_port} to #{@host}:#{@port}"
            source = TCPSocket.new(@host, @port)
            dest = TCPSocket.new(@local, @local_port)
            
            proxies[port] = Proxy.new(source, dest)
          rescue
            puts $!
            puts $!.backtrace.join("\n")
          end
        elsif oper == ?S
          proxy = proxies[port]
          if proxy
            proxy.shutdown
            proxies.delete(port)
          end
        end
      else
        puts "Received bad command: #{cmd}"
        
        # Try to recover
        while command.read(1) != "\n"; end
      end
    end
  end
end

client = ProxyClient.new(ARGV[0], ARGV[1].to_i,
                         ARGV[2].to_i,
                         ARGV[3], ARGV[4].to_i)
