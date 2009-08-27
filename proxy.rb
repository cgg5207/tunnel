require 'socket'

class Proxy
  def set_options(sock)
    sock.setsockopt(Socket::SOL_SOCKET, Socket::SO_REUSEADDR, true)
  end
  
  def initialize(source_addr, source_port, dest_addr, dest_port = nil)
    @source_addr, @source_port, @dest_addr, @dest_port =
      source_addr, source_port, dest_addr, dest_port

    #puts "Proxying from #{@source_addr}:#{@source_port} to #{@dest_addr}:#{@dest_port}"
    
    @source = TCPSocket.new(source_addr, source_port)
    if dest_port == nil
      @dest = dest_addr
    else
      @dest = TCPSocket.new(dest_addr, dest_port)
    end
    
    @shutting_down = false
    @idle = false

    set_options(@source)
    set_options(@dest)
    
    [[@source, @dest], [@dest, @source]].each do |r, w|
      Thread.new do
        while data = r.read(1)
          w.write(data)
        end

        shutdown
      end
    end
  end

  def shutdown_both
    unless @shutting_down
      puts "Shutting down: #{@source.addr.inspect} #{@dest.addr.inspect}"
      @shutting_down = true
      @source.shutdown
      @dest.shutdown unless @idle
    end
    
  rescue
    puts $!
  end

  def shutdown_dest
    unless @idle
      puts "Shutting down dest socket, source is still alive"
      @idle = true
      @dest.shutdown
      @dest = nil
    end
  rescue
    puts $!
  end
end
