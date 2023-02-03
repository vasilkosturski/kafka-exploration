namespace Common;

public static class Utils
{
    public static string GetStateDirectory()
    {
        var currentDir = Environment.CurrentDirectory;
        var solutionDir = Directory.GetParent(currentDir).Parent.Parent.Parent.FullName;
        return Path.Combine(solutionDir, "rocksdb-state");
    }
}