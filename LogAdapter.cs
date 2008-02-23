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

namespace BeIT.MemCached {
	public class LogAdapter {
		public static LogAdapter GetLogger(Type type) {
			return new LogAdapter(type);
		}

		public static LogAdapter GetLogger(string name) {
			return new LogAdapter(name);
		}

		//Console Implementation
		private string loggerName;
		private LogAdapter(string name) { loggerName = name; }
		private LogAdapter(Type type) { loggerName = type.FullName; }
		public void Debug(string message) { Console.Out.WriteLine(DateTime.Now + " DEBUG " + loggerName + " - " + message); }
		public void Info(string message) { Console.Out.WriteLine(DateTime.Now + " INFO " + loggerName + " - " + message); }
		public void Warn(string message) { Console.Out.WriteLine(DateTime.Now + " WARN " + loggerName + " - " + message); }
		public void Error(string message) { Console.Out.WriteLine(DateTime.Now + " ERROR " + loggerName + " - " + message); }
		public void Fatal(string message) { Console.Out.WriteLine(DateTime.Now + " FATAL " + loggerName + " - " + message); }
		public void Debug(string message, Exception e) { Console.Out.WriteLine(DateTime.Now + " DEBUG " + loggerName + " - " + message + "\n" + e.StackTrace); }
		public void Info(string message, Exception e) { Console.Out.WriteLine(DateTime.Now + " INFO " + loggerName + " - " + message + "\n" + e.StackTrace); }
		public void Warn(string message, Exception e) { Console.Out.WriteLine(DateTime.Now + " WARN " + loggerName + " - " + message + "\n" + e.StackTrace); }
		public void Error(string message, Exception e) { Console.Out.WriteLine(DateTime.Now + " ERROR " + loggerName + " - " + message + "\n" + e.StackTrace); }
		public void Fatal(string message, Exception e) { Console.Out.WriteLine(DateTime.Now + " FATAL " + loggerName + " - " + message + "\n" + e.StackTrace); }

		//Log4net Implementation
		/*
		private log4net.ILog logger;
		private LogAdapter(string name) { logger = log4net.LogManager.GetLogger(name); }
		private LogAdapter(Type type) { logger = log4net.LogManager.GetLogger(type); }
		public void Debug(string message) { logger.Debug(message); }
		public void Info(string message) { logger.Info(message); }
		public void Warn(string message) { logger.Warn(message); }
		public void Error(string message) { logger.Error(message); }
		public void Fatal(string message) { logger.Fatal(message); }
		public void Debug(string message, Exception e) { logger.Debug(message, e); }
		public void Info(string message, Exception e) { logger.Info(message, e); }
		public void Warn(string message, Exception e) { logger.Warn(message, e); }
		public void Error(string message, Exception e) { logger.Error(message, e); }
		public void Fatal(string message, Exception e) { logger.Fatal(message, e); }
		*/
	}
}
