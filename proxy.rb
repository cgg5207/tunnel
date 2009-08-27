require 'socket'

class Proxy
  def set_options(sock)
    sock.setsockopt(Socket::SOL_SOCKET, Socket::SO_REUSEADDR, true)
  end
  
  def initialize(source, dest)
    @source, @dest = source, dest
    @shutting_down = false

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

  def shutdown
    unless @shutting_down
      puts "Shutting down: #{@source.addr.inspect} #{@dest.addr.inspect}"
      @shutting_down = true
      @source.shutdown
      @dest.shutdown
    end
    
  rescue
    puts $!
  end
end
