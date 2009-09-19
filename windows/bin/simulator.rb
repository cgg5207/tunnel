
require 'socket'
require 'net/http'
require 'readline'
require 'time'

puts "Waiting for connection..."

server = TCPServer.new(7878)
loop do
  socket = server.accept
  Thread.new do
    while true
      line = socket.read(8)
      exit unless line
      puts "< #{line}"
    end
  end
  
  loop do
    line = Readline::readline('> ')
    Readline::HISTORY.push(line)
    socket.write line
  end
end
