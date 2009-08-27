require 'socket'
require 'thread'

class Proxy
  attr_accessor :index
  
  @@mutex = Mutex.new
  
  def set_options(sock)
    sock.setsockopt(Socket::SOL_SOCKET, Socket::SO_REUSEADDR, true)
  end
  
  def initialize(source, dest, delegate, index)
    @source, @dest, @delegate, @index =
      source, dest, delegate, index
    @shutting_down = false

    set_options(@source)
    set_options(@dest)

    create_threads
  end

  def create_threads
    @threads = [[@source, @dest], [@dest, @source]].map do |r, w|
      Thread.new do
        begin 
          while @dest and !@shutting_down and (data = r.read(1))
            w.write(data)
          end

        rescue Errno::EPIPE
          puts "Closed socket"
        rescue
          puts $!, $!.class
          puts $!.backtrace.join("\n")
        end

        begin
          @@mutex.synchronize do
            if r == @source
              shutdown
            else
              shutdown_dest
            end
            kill_threads
          end
        rescue
          puts $!, $!.class
          puts $!.backtrace.join("\n")
        end
      end
    end
  end

  def kill_threads
    if @threads
      threads, @threads = @threads, nil
      threads.each do |t|
        t.kill unless Thread.current == t
      end
    end
  end

  def reset_dest(dest)
    if @dest
      raise "Can't set dest when one is already present"
    end

    @dest = dest
    set_options(@dest)
    create_threads
  end
  

  def shutdown
    unless @shutting_down
      puts "Shutting down: #{@source.addr.inspect} #{@dest.addr.inspect}"
      @shutting_down = true
      @source.shutdown rescue
      @dest.shutdown rescue

      @delegate.shutdown_remote(self)
    end
    
  rescue
    puts $!
    puts $!.backtrace.join("\n")
  end

  def shutdown_dest
    if @dest and !@shutting_down
      puts "Shutdown dest calling delegate for #{self.index}"
      @delegate.shutdown_remote(self)
      
      puts "Shutting down destination #{@dest.addr.inspect}, source still connected"
      dest, @dest = @dest, nil
      dest.shutdown
    end

  rescue Errno::ENOTCONN
    puts "Ignoring not connected"
  rescue
    puts $!, $!.class
    puts $!.backtrace.join("\n")
  end
end
