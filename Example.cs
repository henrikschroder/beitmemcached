using System;
using System.Collections.Generic;

namespace BeIT.MemCached {
	class Example {
		public static void Main(string[] args) {
			//---------------------
			// Setting up a client.
			//---------------------
			Console.Out.WriteLine("Setting up Memcached Client.");
			MemcachedClient.Setup("MyCache", new string[] { "localhost" });

			//It is possible to have several clients with different configurations:
			//If it is impossible to resolve the hosts, this method will throw an exception.
			try {
				MemcachedClient.Setup("MyOtherCache", new string[]{ "server1.example.com:12345", "server2.example.com:12345"});
			} catch (Exception e) {
				Console.WriteLine(e.Message);
			}

			//Get the instance we just set up so we can use it. You can either store this reference yourself in
			//some field, or fetch it every time you need it, it doesn't really matter.
			MemcachedClient cache = MemcachedClient.GetInstance("MyCache");

			//It is also possible to set up clients in the standard config file. Check the section "beitmemcached" 
			//in the App.config file in this project and you will see that a client called "MyConfigFileCache" is defined.
			MemcachedClient configFileCache = MemcachedClient.GetInstance("MyConfigFileCache");

			//Change client settings to values other than the default like this:
			cache.SendReceieveTimeout = 5000;
			cache.MinPoolSize = 1;
			cache.MaxPoolSize = 5;

			//----------------
			// Using a client.
			//----------------

			//Set some items
			Console.Out.WriteLine("Storing some items.");
			cache.Set("mystring", "The quick brown fox jumped over the lazy dog.");
			cache.Set("myarray", new string[]{"This is the first string.", "This is the second string."});
			cache.Set("myinteger", 4711);
			cache.Set("mydate", new DateTime(2008, 02, 23));

			//Get a string
			string str = cache.Get("mystring") as string;
			if (str != null) {
				Console.Out.WriteLine("Fetched item with key: mystring, value: " + str);
			}

			//Get an object
			string[] array = cache.Get("myarray") as string[];
			if (array != null) {
				Console.Out.WriteLine("Fetched items with key: myarray, value 1: " + array[0] + ", value 2: " + array[1]);
			}

			//Get several values at once
			object[] result = cache.Get(new string[]{"myinteger", "mydate"});
			if (result[0] != null && result[0] is int) {
				Console.Out.WriteLine("Fetched item with key: myinteger, value: " + (int)result[0]);
			}
			if (result[1] != null && result[1] is DateTime) {
				Console.Out.WriteLine("Fetched item with key: mydate, value: " + (DateTime)result[1]);
			}

			//Set a counter
			Console.Out.WriteLine("Setting an item for incrementing and decrementing.");
			cache.SetCounter("mycounter", 9000);
			ulong? counter = cache.GetCounter("mycounter");
			if (counter.HasValue) {
				Console.Out.WriteLine("Fetched mycounter, value: " + counter.Value);
			}

			//Increment the counter
			counter = cache.Increment("mycounter", 1);
			if (counter.HasValue) {
				Console.Out.WriteLine("Incremented mycounter with 1, new value: " + counter.Value);
			}

			//Decrement the counter
			counter = cache.Decrement("mycounter", 9000);
			if (counter.HasValue) {
				Console.Out.WriteLine("Decremented mycounter with 9000, new value: " + counter.Value);
			}

			//Append and prepend
			Console.Out.WriteLine("Storing bar for append/prepend");
			cache.Set("foo", "bar");
			Console.Out.WriteLine("Appending baz");
			cache.Append("foo", " baz");
			Console.Out.WriteLine("Prepending foo");
			cache.Prepend("foo", "foo ");
			Console.Out.WriteLine("New value: " + cache.Get("foo"));

			//Cas
			cache.Delete("castest");
			Console.Out.WriteLine("Trying to CAS non-existant key castest: " + cache.CheckAndSet("castest", "a", 0));
			Console.Out.WriteLine("Setting value for key: castest, value: a");
			cache.Set("castest", "a");
			Console.Out.WriteLine("Trying to CAS key castest with the wrong unique: " + cache.CheckAndSet("castest", "a", 0));
			ulong unique;
			cache.Gets("castest", out unique);
			Console.Out.WriteLine("Getting cas unique for key castest: " + unique);
			Console.Out.WriteLine("Trying to CAS again with the above unique: " + cache.CheckAndSet("castest", "b", unique));
			string value = cache.Gets("castest", out unique) as string;
			Console.Out.WriteLine("New value: " + value + ", new unique: " + unique);

			Console.Out.WriteLine("Displaying the socketpool status:");
			foreach (KeyValuePair<string, Dictionary<string, string>> host in cache.Status()) {
				Console.Out.WriteLine("Host: " + host.Key);
				foreach (KeyValuePair<string, string> item in host.Value) {
					Console.Out.WriteLine("\t" + item.Key + ": " + item.Value);
				}
				Console.Out.WriteLine();
			}

			Console.Out.WriteLine();
			Console.Out.WriteLine("Finished. Press enter to exit.");
			Console.In.ReadLine();
		}
	}
}
