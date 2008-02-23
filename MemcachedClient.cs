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
using System.Configuration;
using System.Globalization;
using System.Text;

namespace BeIT.MemCached{
	public class MemcachedClient{
		#region Static
		//Static hashtable of named instances.
		private static Dictionary<string, MemcachedClient> instances = new Dictionary<string, MemcachedClient>();

		private static LogAdapter logger = LogAdapter.GetLogger(typeof(MemcachedClient));

		//Static method for creating an instance.
		public static void Setup(string name, string[] servers) {
			if (instances.ContainsKey(name)) {
				throw new ConfigurationErrorsException("Trying to configure MemcachedClient instance \"" + name + "\" twice.");
			}
			instances[name] = new MemcachedClient(name, servers);
		}

		public static bool Exists(string name) {
			return instances.ContainsKey(name);
		}

		//Static method for getting a default instance
		private static MemcachedClient defaultInstance = null;
		public static MemcachedClient GetInstance() {
			return defaultInstance ?? (defaultInstance = GetInstance("default"));
		}

		//Static method for getting an instance
		public static MemcachedClient GetInstance(string name) {
			MemcachedClient c;
			if (instances.TryGetValue(name, out c)) {
				return c;
			} else {
				//Try to read configuration and create an instance with this name
				throw new ConfigurationErrorsException("Unable to find MemcachedClient instance \"" + name + "\".");
			}
		}
		#endregion

		#region Fields, constructors and private methods.
		public readonly string Name;
		private readonly ServerPool serverPool;

		/// <summary>
		/// If you specify a key prefix, it will be appended to all keys before they are sent to the memcached server.
		/// They key prefix is not used when calculating which server a key belongs to.
		/// </summary>
		public string KeyPrefix { get { return keyPrefix; } set { keyPrefix = value; } }
		private string keyPrefix = "";

		/// <summary>The send receive timeout is used to determine how long the client should wait for data to be sent and received from the server, 
		/// specified in milliseconds. The default value is 2000.</summary>
		public int SendReceieveTimeout { get { return serverPool.SendReceiveTimeout; } set { serverPool.SendReceiveTimeout = value; } }

		/// <summary>The min pool size determines the number of sockets the connection pool will strive to keep. The default value is 5.</summary>
		public uint MinPoolSize { 
			get { return serverPool.MinPoolSize; } 
			set {
				if (value > MaxPoolSize) { throw new ConfigurationErrorsException("MinPoolSize (" + value + ") may not be larger than the MaxPoolSize (" + MaxPoolSize + ")."); }
				serverPool.MinPoolSize = value;
			} 
		}

		/// <summary>The max pool size determines how large the socket connection pool is allowed to grow, i.e. the maximum of how many free 
		/// connections there can be at a single time. The default value is 10.</summary>
		public uint MaxPoolSize {
			get { return serverPool.MaxPoolSize; } 
			set {
				if (value < MinPoolSize) { throw new ConfigurationErrorsException("MaxPoolSize (" + value + ") may not be smaller than the MinPoolSize (" + MinPoolSize + ")."); }
				serverPool.MaxPoolSize = value;
			}
		}
		
		/// <summary>If the pool contains more than the minimum amount of sockets, and a socket is returned that is older than this recycle time
		/// that socket will be destroyed instead of put back in the pool. This allows the pool to shrink back to the min pool size after a peak in usage.
		/// The default value is 30 minutes.</summary>
		public TimeSpan SocketRecycleAge { get { return serverPool.SocketRecycleAge; } set { serverPool.SocketRecycleAge = value; } }


		//Private constructor
		private MemcachedClient(string name, string[] hosts) {
			if (String.IsNullOrEmpty(name)) {
				throw new ConfigurationErrorsException("Name of MemcachedClient instance cannot be empty.");
			}
			if (hosts == null || hosts.Length == 0) {
				throw new ConfigurationErrorsException("Cannot configure MemcachedClient with empty list of hosts.");
			}

			Name = name;
			serverPool = new ServerPool(hosts);
		}

		//private key hashing method
		private uint hash(string key) {
			checkKey(key);
			return BitConverter.ToUInt32(new ModifiedFNV1_32().ComputeHash(Encoding.UTF8.GetBytes(key)), 0);
		}

		//private multi-hashing method
		private uint[] hash(string[] keys) {
			uint[] result = new uint[keys.Length];
			for (int i = 0; i < keys.Length; i++) {
				result[i] = hash(keys[i]);
			}
			return result;
		}

		//private key-checking method
		//Throws an exception if key contains whitespace or is longer than 250 chars.
		private static void checkKey(string key) {
			if (key == null) {
				throw new ArgumentNullException("Key may not be null.");
			}
			if (key.Length == 0) {
				throw new ArgumentException("Key may not be empty.");
			}
			if (key.Length > 250) {
				throw new ArgumentException("Key may not be longer than 250 characters.");
			}
			if (key.Contains(" ") || key.Contains("\n") || key.Contains("\r") || key.Contains("\t") || key.Contains("\f") || key.Contains("\v")) {
				throw new ArgumentException("Key may not contain whitespace or control characters.");
			}
		}

		//Private Unix-time converter
		private static DateTime epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
		private static int getUnixTime(DateTime datetime) {
			return (int)(datetime.ToUniversalTime() - epoch).TotalSeconds;
		}
		#endregion

		#region Set
		//Set
		public bool Set(string key, object value) { return store("set", key, true, value, hash(key), 0); }
		public bool Set(string key, object value, uint hash) { return store("set", key, false, value, hash, 0); }
		public bool Set(string key, object value, TimeSpan expiry) { return store("set", key, true, value, hash(key), (int)expiry.TotalSeconds); }
		public bool Set(string key, object value, uint hash, TimeSpan expiry) { return store("set", key, false, value, hash, (int)expiry.TotalSeconds); }
		public bool Set(string key, object value, DateTime expiry) { return store("set", key, true, value, hash(key), getUnixTime(expiry)); }
		public bool Set(string key, object value, uint hash, DateTime expiry) { return store("set", key, false, value, hash, getUnixTime(expiry)); }

		//Add
		public bool Add(string key, object value) { return store("add", key, true, value, hash(key), 0); }
		public bool Add(string key, object value, uint hash) { return store("add", key, false, value, hash, 0); }
		public bool Add(string key, object value, TimeSpan expiry) { return store("add", key, true, value, hash(key), (int)expiry.TotalSeconds); }
		public bool Add(string key, object value, uint hash, TimeSpan expiry) { return store("add", key, false, value, hash, (int)expiry.TotalSeconds); }
		public bool Add(string key, object value, DateTime expiry) { return store("add", key, true, value, hash(key), getUnixTime(expiry)); }
		public bool Add(string key, object value, uint hash, DateTime expiry) { return store("add", key, false, value, hash, getUnixTime(expiry)); }

		//Replace
		public bool Replace(string key, object value) { return store("replace", key, true, value, hash(key), 0); }
		public bool Replace(string key, object value, uint hash) { return store("replace", key, false, value, hash, 0); }
		public bool Replace(string key, object value, TimeSpan expiry) { return store("replace", key, true, value, hash(key), (int)expiry.TotalSeconds); }
		public bool Replace(string key, object value, uint hash, TimeSpan expiry) { return store("replace", key, false, value, hash, (int)expiry.TotalSeconds); }
		public bool Replace(string key, object value, DateTime expiry) { return store("replace", key, true, value, hash(key), getUnixTime(expiry)); }
		public bool Replace(string key, object value, uint hash, DateTime expiry) { return store("replace", key, false, value, hash, getUnixTime(expiry)); }

		private bool store(string command, string key, bool keyIsChecked, object value, uint hash, int expiry) {
			if (!keyIsChecked) {
				checkKey(key);
			}

			return serverPool.Execute(hash, false, delegate(PooledSocket socket){
				SerializedType type;
				byte[] bytes;

				//Serialize object efficiently, store the datatype marker in the flags property.
				try {
					bytes = Serializer.Serialize(value, out type);
				}
				catch (Exception e) {
					//If serialization fails, return false;
					
					logger.Error("Error serializing object", e);
					return false;
				}

				//Create commandline
				string commandline = command + " " + keyPrefix + key + " " + (ushort)type + " " + expiry + " " + bytes.Length + "\r\n";

				//Write commandline and serialized object.
				socket.Write(commandline);
				socket.Write(bytes);
				socket.Write("\r\n");
				return socket.ReadResponse().StartsWith("STORED");
			});
		}
		#endregion

		#region Get
		public object Get(string key) { return get(key, true, hash(key)); }
		public object Get(string key, uint hash) { return get(key, false, hash); }

		private object get(string key, bool keyIsChecked, uint hash) {
			if (!keyIsChecked) {
				checkKey(key);
			}

			return serverPool.Execute<object>(hash, null, delegate(PooledSocket socket){
				socket.Write("get " + keyPrefix + key + "\r\n");
				object value;
				if (readValue(socket, out value, out key)) {
					socket.ReadLine(); //Read the trailing END.
				}
				return value;
			});
		}

		public object[] Get(string[] keys) { return get(keys, true, hash(keys)); }
		public object[] Get(string[] keys, uint[] hashes) { return get(keys, false, hashes); }

		private object[] get(string[] keys, bool keysAreChecked, uint[] hashes) {
			//Check arguments.
			if (keys == null || hashes == null) {
				throw new ArgumentException("Keys and hashes arrays must not be null.");
			}
			if (keys.Length != hashes.Length) {
				throw new ArgumentException("Keys and hashes arrays must be of the same length.");
			}

			//Avoid going through the server grouping if there's only one key.
			if (keys.Length == 1) {
				return new object[] { get(keys[0], keysAreChecked, hashes[0]) };
			}

			//Check keys.
			if (!keysAreChecked) {
				for (int i = 0; i < keys.Length; i++) {
					checkKey(keys[i]);
				}
			}

			//Group the keys/hashes by server(pool)
			Dictionary<SocketPool, Dictionary<string, List<int>>> dict = new Dictionary<SocketPool, Dictionary<string, List<int>>>();
			for (int i = 0; i < keys.Length; i++) {
				Dictionary<string, List<int>> getsForServer;
				SocketPool pool = serverPool.GetSocketPool(hashes[i]);
				if (!dict.TryGetValue(pool, out getsForServer)) {
					dict[pool] = getsForServer = new Dictionary<string, List<int>>();
				} 

				List<int> positions;
				if(!getsForServer.TryGetValue(keys[i], out positions)){
					getsForServer[keys[i]] = positions = new List<int>();
				}
				positions.Add(i);
			}

			//Get the values
			object[] returnValues = new object[keys.Length];
			foreach (KeyValuePair<SocketPool, Dictionary<string, List<int>>> kv in dict) {
				serverPool.Execute(kv.Key, delegate(PooledSocket socket){
					//Build the get request
					StringBuilder getRequest = new StringBuilder("get");
					foreach (KeyValuePair<string, List<int>> key in kv.Value) {
						getRequest.Append(" ");
						getRequest.Append(keyPrefix);
						getRequest.Append(key.Key);
					}
					getRequest.Append("\r\n");

					//Send get request
					socket.Write(getRequest.ToString());

					//Read values, one by one
					object gottenObject;
					string gottenKey;
					while (readValue(socket, out gottenObject, out gottenKey)) {
						foreach(int position in kv.Value[gottenKey]) {
							returnValues[position] = gottenObject;
						}
					}
				});
			}

			return returnValues;
		}

		private bool readValue(PooledSocket socket, out object value, out string key) {
			string response = socket.ReadResponse();
			string[] parts = response.Split(' '); //Result line from server: "VALUE key flags bytes"
			if (parts[0] == "VALUE") {
				key = parts[1];
				SerializedType type = (SerializedType)Enum.Parse(typeof(SerializedType), parts[2]);
				byte[] bytes = new byte[Convert.ToInt32(parts[3], CultureInfo.InvariantCulture)];
				socket.Read(bytes);
				socket.SkipUntilEndOfLine(); //Skip the trailing \r\n
				try {
					value = Serializer.DeSerialize(bytes, type);
				} catch (Exception e) {
					//If serialization fails, return null
				    value = null;
					logger.Error("Error deserializing object", e);
				}
				return true;
			} else {
				key = null;
				value = null;
				return false;
			}
		}
		#endregion

		#region Delete
		public bool Delete(string key) { return delete(key, true, hash(key), 0); }
		public bool Delete(string key, uint hash) { return delete(key, false, hash, 0); }
		public bool Delete(string key, TimeSpan time) { return delete(key, true, hash(key), (int)time.TotalSeconds); }
		public bool Delete(string key, uint hash, TimeSpan time) { return delete(key, false, hash, (int)time.TotalSeconds);	}
		public bool Delete(string key, DateTime time) {	return delete(key, true, hash(key), getUnixTime(time)); }
		public bool Delete(string key, uint hash, DateTime time) {	return delete(key, false, hash, getUnixTime(time));	}

		private bool delete(string key, bool keyIsChecked, uint hash, int time) {
			if (!keyIsChecked) {
				checkKey(key);
			}

			return serverPool.Execute(hash, false, delegate(PooledSocket socket){
				string commandline;
				if (time == 0) {
					commandline = "delete " + keyPrefix + key + "\r\n";
				} else {
					commandline = "delete " + keyPrefix + key + " " + time + "\r\n";
				}
				socket.Write(commandline);
				return socket.ReadResponse().StartsWith("DELETED");
			});
		}
		#endregion

		#region Increment Decrement
		//The setcounter methods set values on the server such that they can be 
		//incremented or decremented on the server, i.e. as a decimal representation of a 64-bit unsigned integer.
		//TODO: Expiry overloads
		public bool SetCounter(string key, ulong value) { return Set(key, value.ToString(CultureInfo.InvariantCulture)); }
		public bool SetCounter(string key, ulong value, uint hash) { return Set(key, value.ToString(CultureInfo.InvariantCulture), hash); }

		public ulong? GetCounter(string key) {return getCounter(key, true, hash(key));}
		public ulong? GetCounter(string key, uint hash) { return getCounter(key, false, hash); }

		private ulong? getCounter(string key, bool keyIsChecked, uint hash) {
			ulong parsedLong;
			return ulong.TryParse(get(key, keyIsChecked, hash) as string, out parsedLong) ? (ulong?)parsedLong : null;
		}

		public ulong?[] GetCounter(string[] keys) {return getCounter(keys, true, hash(keys));}
		public ulong?[] GetCounter(string[] keys, uint[] hashes) { return getCounter(keys, false, hashes); }

		private ulong?[] getCounter(string[] keys, bool keysAreChecked, uint[] hashes) {
			ulong?[] results = new ulong?[keys.Length];
			object[] values = get(keys, keysAreChecked, hashes);
			for (int i = 0; i < values.Length; i++) {
				ulong parsedLong;
				results[i] = ulong.TryParse(values[i] as string, out parsedLong) ? (ulong?)parsedLong : null;
			}
			return results;
		}

		//The increment method increments an existing value on the server with the given value and returns the result.
		//For this to work, the value on the server must be a decimal representation of a 64-bit unsigned integer.
		public ulong? Increment(string key, ulong value) { return incrementDecrement("incr", key, true, value, hash(key)); }
		public ulong? Increment(string key, ulong value, uint hash) { return incrementDecrement("incr", key, false, value, hash); }

		public ulong? Decrement(string key, ulong value) { return incrementDecrement("decr", key, true, value, hash(key)); }
		public ulong? Decrement(string key, ulong value, uint hash) { return incrementDecrement("decr", key, false, value, hash); }

		private ulong? incrementDecrement(string cmd, string key, bool keyIsChecked, ulong value, uint hash) {
			if (!keyIsChecked) {
				checkKey(key);
			}
			return serverPool.Execute<ulong?>(hash, null, delegate(PooledSocket socket) {
				string command = cmd + " " + keyPrefix + key + " " + value + "\r\n";
				socket.Write(command);
				string response = socket.ReadResponse();
				if (response.StartsWith("NOT_FOUND")) {
					return null;
				} else {
					return Convert.ToUInt64(response.TrimEnd('\0', '\r', '\n'));
				}
			});
		}
		#endregion

		#region Flush All
		//Flush_all
		public bool FlushAll() {
			bool noerrors = true;
			serverPool.ExecuteAll(delegate(PooledSocket socket){
				socket.Write("flush_all\r\n");
				if (!socket.ReadResponse().StartsWith("OK")) {
					noerrors = false;
				}
			});
			return noerrors;
		}
		#endregion

		#region Stats
		//Stats, should probably be made to return stats from one server or all servers.
		public Dictionary<string, Dictionary<string, string>> Stats() {
			Dictionary<string, Dictionary<string, string>> results = new Dictionary<string, Dictionary<string, string>>();
			serverPool.ExecuteAll(delegate(PooledSocket socket){
				Dictionary<string, string> result = new Dictionary<string,string>();
				//TODO: //results[socket.Host] = result; 

				socket.Write("stats\r\n");
				string line;
				while (!(line = socket.ReadResponse().TrimEnd('\0', '\r', '\n')).StartsWith("END")) {
					string[] s = line.Split(' ');
					result.Add(s[1], s[2]);
				}
			});
			return results;
		}
		#endregion

		#region Status
		//Status, checks the connectivity for each server.
		public Dictionary<string, string> Status() {
			return serverPool.Status();
		}
		#endregion
	}
}