using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Parquet;

sealed class OrderedCommitGate {
    public record struct Token(int Id);

    int _committedContiguous = 0;
    int _currentId = 0;
    readonly HashSet<int> _completed = [];
    readonly Dictionary<int, TaskCompletionSource<object?>> _waiters = [];

    public Token QueueForCommit() => new(_currentId++);

    public Task WaitForCommit(Token commitToken) {
        lock(_waiters) {
            int previousCommitId = commitToken.Id - 1;
            if(previousCommitId <= _committedContiguous)
                return Task.CompletedTask;
            var tcs = new TaskCompletionSource<object?>();
            _waiters[commitToken.Id] = tcs;
            return tcs.Task;
        }
    }

    public void CommitDone(Token commitToken) {
        lock(_waiters) {
            _completed.Add(commitToken.Id);

            // advance contiguous prefix
            while(_completed.Contains(_committedContiguous + 1)) {
                _completed.Remove(_committedContiguous + 1);
                _committedContiguous++;
            }

            if(_waiters.Count == 0)
                return;

            // release any waiters whose prerequisite is now satisfied
            int[] ready = _waiters.Keys
                .Where(id => id - 1 <= _committedContiguous)
                .ToArray();
            foreach(int id in ready) {
                if(_waiters.TryGetValue(id, out TaskCompletionSource<object?>? tcs)) {
                    _waiters.Remove(id);
                    tcs?.SetResult(null);
                }
            }
        }
    }
}
