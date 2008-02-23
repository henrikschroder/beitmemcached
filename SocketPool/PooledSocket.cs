//Copyright (c) 2007-2008 Henrik Schröder, Oliver Kofoed Pedersen

//Permission is hereby granted, free of charge, to any person
//obtaining a copy of this software and associated documentation
//files (the "Software"), to deal in the Software without
//restriction, including without limitation the rights to use,
//copy, modify, merge, publish, distribute, sublicense, and/or sell
//copies of the Software, and to permit persons to whom the
//Software is furnished to do so, subject to the following
//conditions:

//The above copyright notice and this permission notice shall be
//included in all copies or substantial portions of the Software.

//THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
//EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
//OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
//NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
//HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
//WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
//FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
//OTHER DEALINGS IN THE SOFTWARE.

using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace BeIT.MemCached {
	internal class PooledSocket : IDisposable {
		private static LogAdapter logger = LogAdapter.GetLogger(typeof(PooledSocket));
		
		private SocketPool socketPool;
		private Socket socket;
		private Stream stream;
		public readonly DateTime Created;

		public PooledSocket(SocketPool socketPool, IPEndPoint endPoint, int sendReceiveTimeout) {
			this.socketPool = socketPool;
			Created = DateTime.Now;
			socket = new Socket(endPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
			socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.SendTimeout, sendReceiveTimeout);
			socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReceiveTimeout, sendReceiveTimeout);
			socket.ReceiveTimeout = sendReceiveTimeout;
			socket.SendTimeout = sendReceiveTimeout;

			//Do not use Nagle's Algorithm
			socket.NoDelay = true;

			//Establish connection
			socket.Connect(endPoint);

			stream = new BufferedStream(new NetworkStream(socket, false));
		}

		public void Dispose() {
			socketPool.Return(this);
		}

		public void Close() {
			if (stream != null) {
				try { stream.Close(); } catch (Exception e) { logger.Error("Error closing stream: " + socketPool.Host, e); }
				stream = null;
			}
			if (socket != null ) {
				try { socket.Shutdown(SocketShutdown.Both); } catch (Exception e) { logger.Error("Error shutting down socket: " + socketPool.Host, e);}
				try { socket.Close(); } catch (Exception e) { logger.Error("Error closing socket: " + socketPool.Host, e);}
				socket = null;
			}
		}

		public bool IsAlive {
			get { return socket != null && socket.Connected && stream.CanRead; }
		}

		public void Write(string str) {
			Write(Encoding.UTF8.GetBytes(str));
		}

		public void Write(byte[] bytes) {
			stream.Write(bytes, 0, bytes.Length);
			stream.Flush();
		}

		//Reads until \r\n, but does not return those characters.
		public string ReadLine() {
			MemoryStream buffer = new MemoryStream();
			int b;
			bool gotReturn = false;
			while((b = stream.ReadByte()) != -1) {
				if(gotReturn) {
					if(b == 10) {
						break;
					} else {
						buffer.WriteByte(13);
						gotReturn = false;
					}
				}
				if(b == 13) {
					gotReturn = true;
				} else {
					buffer.WriteByte((byte)b);
				}
			}
			return Encoding.UTF8.GetString(buffer.GetBuffer());
		}

		//Reads a response line, checks for general errors, and returns the line.
		public string ReadResponse() {
			string response = ReadLine();

			if(String.IsNullOrEmpty(response)) {
				throw new MemcachedClientException("Received empty response.");
			}

			if(response.StartsWith("ERROR")
				|| response.StartsWith("CLIENT_ERROR")
				|| response.StartsWith("SERVER_ERROR")) {
				throw new MemcachedClientException("Server returned " + response);
			}

			return response;
		}

		//Fills the array from the stream.
		public void Read(byte[] bytes) {
			if(bytes == null) {
				return;
			}

			int readBytes = 0;
			while(readBytes < bytes.Length) {
				readBytes += stream.Read(bytes, readBytes, (bytes.Length - readBytes));
			}
		}

		public void SkipUntilEndOfLine() {
			int b;
			bool gotReturn = false;
			while((b = stream.ReadByte()) != -1) {
				if(gotReturn) {
					if(b == 10) {
						break;
					} else {
						gotReturn = false;
					}
				}
				if(b == 13) {
					gotReturn = true;
				}
			}
		}

		//Empties all buffers and makes sure the socket is empty of received data.
		//If there was any leftover data, this method will return true, otherwise false.
		public bool Reset() {
			if (socket.Available > 0) {
				byte[] b = new byte[socket.Available];
				Read(b);
				return true;
			}
			return false;
		}
	}
}