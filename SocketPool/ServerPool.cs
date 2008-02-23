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
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Threading;

namespace BeIT.MemCached {
	internal delegate T UseSocket<T>(PooledSocket socket);
	internal delegate void UseSocket(PooledSocket socket);

	internal class ServerPool {
		private static LogAdapter logger = LogAdapter.GetLogger(typeof(ServerPool));

		private SocketPool[] hostList;

		private int sendReceiveTimeout = 2000;
		private uint maxPoolSize = 10;
		private uint minPoolSize = 5;
		private TimeSpan socketRecycleAge = TimeSpan.FromMinutes(30);
		internal int SendReceiveTimeout { get { return sendReceiveTimeout; } set { sendReceiveTimeout = value; } }
		internal uint MaxPoolSize { get { return maxPoolSize; } set { maxPoolSize = value; } }
		internal uint MinPoolSize { get { return minPoolSize; } set { minPoolSize = value; } }
		internal TimeSpan SocketRecycleAge { get { return socketRecycleAge; } set { socketRecycleAge = value; } }

		internal ServerPool(string[] hosts) {
			List<SocketPool> pools = new List<SocketPool>();
			foreach(string host in hosts) {
				pools.Add(new SocketPool(this, host));
			}
			hostList = pools.ToArray();

			setupSocketPoolKeys();
		}

		private Dictionary<uint, SocketPool> hostDictionary;
		private uint[] hostKeys;
		private void setupSocketPoolKeys() {
			hostDictionary = new Dictionary<uint, SocketPool>();
			List<uint> keys = new List<uint>();
			foreach (SocketPool pool in hostList) {
				string str = pool.Host;
				for(int i = 0; i < 30; i++) {
					//To get a good distribution of hashes for each host, we start by hashing the name of the host, then we iteratively hash the result until we have the wanted number of hashes.
					uint key = BitConverter.ToUInt32(new ModifiedFNV1_32().ComputeHash(Encoding.UTF8.GetBytes(str)), 0);
					if (!hostDictionary.ContainsKey(key)) {
						hostDictionary[key] = pool;
						keys.Add(key);
					}
					str = key.ToString(CultureInfo.InvariantCulture);
				}
			}
			keys.Sort();
			hostKeys = keys.ToArray();
		}

		internal SocketPool GetSocketPool(uint hash) {
			//Old, simple host selection:
			//return hostList[(int)(hash % hostList.Length)];
			
			//Quick return if we only have one host.
			if (hostList.Length == 1) {
				return hostList[0];
			}

			//New "ketama" host selection.
			int i = Array.BinarySearch(hostKeys, hash);

			//If not exact match...
			if(i < 0) {
				//Get the index of the first item bigger than the one searched for.
				i = ~i;

				//If i is bigger than the last index, it was bigger than the last item = use the first item.
				if (i >= hostKeys.Length) {
					i = 0;
				}
			}
			return hostDictionary[hostKeys[i]];
		}

		internal T Execute<T>(uint hash, T defaultValue, UseSocket<T> use) {
			return Execute(GetSocketPool(hash), defaultValue, use);
		}

		public static int InExecuteCounter = 0;
		internal T Execute<T>(SocketPool pool, T defaultValue, UseSocket<T> use) {
			PooledSocket sock = null;
			Interlocked.Increment(ref InExecuteCounter);
			try {
				sock = pool.Acquire();
				if (sock != null) {
					return use(sock);
				}
			} catch(Exception e) {
				logger.Error("Error in Execute<T>: " + pool.Host, e);

				//Socket is probably broken
				if (sock != null) {
					sock.Close();
				}
			} finally {
				Interlocked.Decrement(ref InExecuteCounter);
				if (sock != null) {
					sock.Dispose();
				}
			}
			return defaultValue;
		}

		internal void Execute(SocketPool pool, UseSocket use) {
			PooledSocket sock = null;
			Interlocked.Increment(ref InExecuteCounter);
			try {
				sock = pool.Acquire();
				if (sock != null) {
					use(sock);
				}
			} catch(Exception e) {
				logger.Error("Error in Execute: " + pool.Host, e);

				//Socket is probably broken
				if (sock != null) {
					sock.Close();
				}
			}
			finally {
				Interlocked.Decrement(ref InExecuteCounter);
				if(sock != null) {
					sock.Dispose();
				}
			}
		}

		internal void ExecuteAll(UseSocket use) {
			foreach(SocketPool socketPool in hostList){
				Execute(socketPool, use);
			}
		}

		internal Dictionary<string, string> Status() {
			Dictionary<string, string> result = new Dictionary<string, string>();
			result.Add("<b>General</b>", "Current execute counter: " + InExecuteCounter);
			foreach (SocketPool socketPool in hostList) {
				string str = "";
				if (Execute(socketPool, false, delegate{ return true; })) {
					str = "\tStatus:\t\t\tOk\n";
				} else {
					str = "\tStatus:\t\t\tDead, next retry at: " + socketPool.DeadEndPointRetryTime + "\n";
				}
				str += "\tSockets in pool:\t" + socketPool.Poolsize + "\n";
				str += "\tAcquired sockets:\t" + socketPool.Acquired + "\n";
				str += "\tNew sockets created:\t" + socketPool.NewSockets + "\n";
				str += "\tNew sockets failed:\t" + socketPool.FailedNewSockets + "\n";
				str += "\tSockets reused:\t\t" + socketPool.ReusedSockets + "\n";
				str += "\tSockets died in pool:\t" + socketPool.DeadSocketsInPool + "\n";
				str += "\tSockets died on return:\t" + socketPool.DeadSocketsOnReturn + "\n";
				str += "\tDirty sockets return:\t" + socketPool.DirtySocketsOnReturn + "\n";
				result.Add("<b>" + socketPool.Host + "</b>", str);
			}
			return result;
		}
	}
}