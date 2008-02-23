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

	/// <summary>
	/// The ServerPool encapsulates a collection of memcached servers and the associated SocketPool objects.
	/// This class contains the server-selection logic, and contains methods for executing a block of code on 
	/// a socket from the server corresponding to a given key.
	/// </summary>
	internal class ServerPool {
		private static LogAdapter logger = LogAdapter.GetLogger(typeof(ServerPool));

		private SocketPool[] hostList;
		private Dictionary<uint, SocketPool> hostDictionary;
		private uint[] hostKeys;

		//Internal configuration properties
		private int sendReceiveTimeout = 2000;
		private uint maxPoolSize = 10;
		private uint minPoolSize = 5;
		private TimeSpan socketRecycleAge = TimeSpan.FromMinutes(30);
		internal int SendReceiveTimeout { get { return sendReceiveTimeout; } set { sendReceiveTimeout = value; } }
		internal uint MaxPoolSize { get { return maxPoolSize; } set { maxPoolSize = value; } }
		internal uint MinPoolSize { get { return minPoolSize; } set { minPoolSize = value; } }
		internal TimeSpan SocketRecycleAge { get { return socketRecycleAge; } set { socketRecycleAge = value; } }

		/// <summary>
		/// Internal constructor. This method takes the array of hosts and sets up an internal list of socketpools.
		/// </summary>
		internal ServerPool(string[] hosts) {
			hostDictionary = new Dictionary<uint, SocketPool>();
			List<SocketPool> pools = new List<SocketPool>();
			List<uint> keys = new List<uint>();
			foreach(string host in hosts) {
				//Create pool
				SocketPool pool = new SocketPool(this, host);

				//Create 30 keys for this pool, store each key in the hostDictionary, as well as in the list of keys.
				string str = host;
				for (int i = 0; i < 30; i++) {
					//To get a good distribution of hashes for each host, we start by hashing the name of the host, then we iteratively hash the result until we have the wanted number of hashes.
					uint key = BitConverter.ToUInt32(new ModifiedFNV1_32().ComputeHash(Encoding.UTF8.GetBytes(str)), 0);
					if (!hostDictionary.ContainsKey(key)) {
						hostDictionary[key] = pool;
						keys.Add(key);
					}
					str = key.ToString(CultureInfo.InvariantCulture);
				}

				pools.Add(pool);
			}

			//Hostlist should contain the list of all pools that has been created.
			hostList = pools.ToArray();

			//Hostkeys should contain the list of all key for all pools that have been created.
			//This array forms the server key continuum that we use to lookup which server a
			//given item key hash should be assigned to.
			keys.Sort();
			hostKeys = keys.ToArray();

		}

		/// <summary>
		/// Given a item key hash, this method returns the serverpool which is closest on the server key continuum.
		/// </summary>
		internal SocketPool GetSocketPool(uint hash) {
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

		//Debug field
		public static int InExecuteCounter = 0;

		/// <summary>
		/// This method executes the given delegate on a socket from the server that corresponds to the given hash.
		/// If anything causes an error, the given defaultValue will be returned instead.
		/// This method takes care of disposing the socket properly once the delegate has executed.
		/// </summary>
		internal T Execute<T>(uint hash, T defaultValue, UseSocket<T> use) {
			return Execute(GetSocketPool(hash), defaultValue, use);
		}

		internal T Execute<T>(SocketPool pool, T defaultValue, UseSocket<T> use) {
			PooledSocket sock = null;
			Interlocked.Increment(ref InExecuteCounter);
			try {
				//Acquire a socket
				sock = pool.Acquire();

				//Use the socket as a parameter to the delegate and return its result.
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
				//Acquire a socket
				sock = pool.Acquire();

				//Use the socket as a parameter to the delegate and return its result.
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

		/// <summary>
		/// This method executes the given delegate on all servers.
		/// </summary>
		internal void ExecuteAll(UseSocket use) {
			foreach(SocketPool socketPool in hostList){
				Execute(socketPool, use);
			}
		}

		/// <summary>
		/// This method checks the status of each server and returns a Dictionary with usage statistics
		/// for each server.
		/// </summary>
		internal Dictionary<string, string> Status() {
			Dictionary<string, string> result = new Dictionary<string, string>();
			result.Add("<b>General</b>", "Current execute counter: " + InExecuteCounter);
			foreach (SocketPool socketPool in hostList) {
				string str;
				if (Execute<bool>(socketPool, false, delegate{ return true; })) {
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