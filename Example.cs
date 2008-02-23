using System;
using BeIT.MemCached;

namespace BeITMemcached {
	class Example {
		public static void Main(string[] args) {
			Console.Out.WriteLine("Setting up Memcached Client.");			
			//Set up a client.
			MemcachedClient.Setup("MyCache", new string[] { "localhost" });

			//It is possible to have several clients with different configurations:
			//If it is impossible to resolve the hosts, this method will throw an exception.
			//MemcachedClient.Setup("MyOtherCache", new string[]{ "server1.example.com", "server2.example.com"});

			//Get the instance we just set up so we can use it. You can either store this reference yourself in
			//some field, or fetch it every time you need it, it doesn't really matter.
			MemcachedClient cache = MemcachedClient.GetInstance("MyCache");

			//Change client settings to values other than the default like this:
			cache.SendReceieveTimeout = 5000;
			cache.MinPoolSize = 1;
			cache.MaxPoolSize = 5;

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

			Console.Out.WriteLine("Setting an item for incrementing and decrementing.");
			cache.SetCounter("mycounter", 9000);
			ulong? counter = cache.GetCounter("mycounter");
			if (counter.HasValue) {
				Console.Out.WriteLine("Fetched mycounter, value: " + counter.Value);
			}

			counter = cache.Increment("mycounter", 1);
			if (counter.HasValue) {
				Console.Out.WriteLine("Incremented mycounter with 1, new value: " + counter.Value);
			}

			counter = cache.Decrement("mycounter", 9000);
			if (counter.HasValue) {
				Console.Out.WriteLine("Decremented mycounter with 9000, new value: " + counter.Value);
			}

			Console.Out.WriteLine();
			Console.Out.WriteLine("Finished. Press enter to exit.");
			Console.In.ReadLine();
		}
	}
}
