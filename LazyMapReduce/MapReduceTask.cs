using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Collections.Concurrent;

namespace LazyMapReduce
{
	// can be replaced with a normal KeyValuePair but done like that for consistency
	public struct KeySingleValuePair <TKey, TVal>
	{
		public TKey key;
		public TVal value;
	}

	public struct KeyMultipleValuesPair <TKey, TVal>
	{
		public TKey key;
		public List<TVal> values;
	}

	/// <summary>
	/// A typless version of MapReduceTask, which assumes that all of its types are object.
	/// It makes initializing a task simpler but it requires casting to apply certain operations
	/// on input/output keys/values.
	/// </summary>
	public class TypelessMapReduce: MapReduceTask<object, object, object, object, object, object>
	{
	}

	/// <summary>
	/// A lazy, yet no so bad for educational purpose, implementation of MapReduce. It is not meant for any
	/// production level implementation, it's just a playground for those who want to have fun implementing
	/// anything in MapReduce, or those who just want to try it out.
	/// 
	/// For a typless class, which assumes that all types are 'object' use TypelessMapReduce.
	/// </summary>
	public class MapReduceTask<TMapInputKey, TMapInputVal, TMapOutputKey, TMapOutputVal, TReduceOutputKey, TReduceOutputVal>
	{
		public delegate List<KeySingleValuePair<TMapOutputKey, TMapOutputVal>> MapDelegate (KeySingleValuePair<TMapInputKey, TMapInputVal> input);
		public delegate KeySingleValuePair<TReduceOutputKey, TReduceOutputVal> ReduceDelegate (KeyMultipleValuesPair<TMapOutputKey, TMapOutputVal> input);

		public MapDelegate Map;
		public ReduceDelegate Reduce;

		ConcurrentQueue<KeySingleValuePair<TMapInputKey, TMapInputVal>> mapInputs = new ConcurrentQueue<KeySingleValuePair<TMapInputKey, TMapInputVal>> ();
		ConcurrentQueue<KeySingleValuePair<TMapOutputKey, TMapOutputVal>> mapOutputs = new ConcurrentQueue<KeySingleValuePair<TMapOutputKey, TMapOutputVal>> ();
		ConcurrentQueue<KeyMultipleValuesPair<TMapOutputKey, TMapOutputVal>> reduceInputs = new ConcurrentQueue<KeyMultipleValuesPair<TMapOutputKey, TMapOutputVal>> ();

		// use concurrent bag for handling synchronization when accessed by different threads
		protected ConcurrentBag<KeySingleValuePair<TReduceOutputKey, TReduceOutputVal>> results;

		protected int numMapTasks = 0;
		protected int numReduceTasks = 0;

		public void PushInput (TMapInputKey mapInputKey, TMapInputVal mapInputValue)
		{
			PushInput (new KeySingleValuePair<TMapInputKey, TMapInputVal> {
				key = mapInputKey,
				value = mapInputValue
			});
		}

		public void PushInput (KeySingleValuePair<TMapInputKey, TMapInputVal> input)
		{
			mapInputs.Enqueue (input);
			numMapTasks++;
		}

		public ConcurrentBag<KeySingleValuePair<TReduceOutputKey, TReduceOutputVal>> GetResults ()
		{
			return results;
		}

		public void Run ()
		{
			Run (false, false);
		}

		public void Run (bool parallelMap, bool parallelReduce)
		{
			RunMaps (parallelMap);
			Shuffle ();
			RunReduces (parallelReduce);

			// reset the counters
			numMapTasks = 0;
			numReduceTasks = 0;
		}

		protected void RunMaps (bool parallelMap)
		{
			Console.WriteLine ("\n\nMapping...");

			Action<int> handler = i => {
				Console.WriteLine ("Map task {0}/{1}: executing ...", i+1, numMapTasks);

				KeySingleValuePair<TMapInputKey, TMapInputVal> input;

				if (!mapInputs.TryDequeue (out input)) {
					Console.WriteLine ("Map task {0}/{1}: Failed to extract input.\nSkipping this task", i+1, numMapTasks);
					return;
				}

				var mapOutput = Map (input);
				var enumerator = mapOutput.GetEnumerator ();
				while (enumerator.MoveNext ()) {
					mapOutputs.Enqueue (enumerator.Current);
				}

				Console.WriteLine ("Map task {0}/{1}: done ({2} outputs)", i+1, numMapTasks, mapOutput.Count);
			};
				
			if (parallelMap) {
				Parallel.For (0, numMapTasks, handler);
			} else {
				var count = 0;
				while (mapInputs.Count != 0) {
					handler.Invoke (count++);
				}
			}

			Console.WriteLine ("Finished mapping phase");
		}

		protected void Shuffle ()
		{
			Console.WriteLine ("\n\nShuffling...");
			var shuffles = new Dictionary<TMapOutputKey, List<TMapOutputVal>> ();
			var enumerator = mapOutputs.GetEnumerator ();

			Console.WriteLine ("Grouping values under keys...");
			while (enumerator.MoveNext ()) {
				if (!shuffles.ContainsKey (enumerator.Current.key))
					shuffles.Add (enumerator.Current.key, new List<TMapOutputVal> ());
				shuffles [enumerator.Current.key].Add (enumerator.Current.value);
			}

			Console.WriteLine ("Preparing reduce inputs");
			foreach (TMapOutputKey mapOutputKey in shuffles.Keys) {
				reduceInputs.Enqueue (new KeyMultipleValuesPair<TMapOutputKey, TMapOutputVal> {
					key = mapOutputKey,
					values = shuffles[mapOutputKey]
				});
				numReduceTasks++;
			}

			Console.WriteLine ("Finished shuffling phase");
		}

		protected void RunReduces (bool parallelReduce)
		{
			Console.WriteLine ("\n\nReducing...");
			results = new ConcurrentBag<KeySingleValuePair<TReduceOutputKey, TReduceOutputVal>> ();

			Action<int> handler = i => {
				Console.WriteLine ("Reduce task {0}/{1}: executing ...", i, numReduceTasks);

				KeyMultipleValuesPair<TMapOutputKey, TMapOutputVal> input;

				if (!reduceInputs.TryDequeue (out input)) {
					Console.WriteLine ("Reduce task {0}/{1}: Failed to extracting input.\nSkipping this task", i, numReduceTasks);
					return;
				}

				results.Add(Reduce (input));

				Console.WriteLine ("Reduce task {0}/{1}: done", i, numReduceTasks);
			};

			if (parallelReduce) {
				Parallel.For (0, numReduceTasks, handler);
			} else {
				int count = 0;
				while (reduceInputs.Count != 0) {
					handler (count++);
				}
			}

			Console.WriteLine ("Finished reducing phase\n\n");
		}
	}
}