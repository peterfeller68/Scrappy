using System;
using System.Collections.Generic;

#nullable disable
namespace AzureDataImportLibrary
{
    public static class Utilities
    {
        public static string GetEnvironmentVariable(string name)
        {
            return Environment.GetEnvironmentVariable(name, (EnvironmentVariableTarget)0);
        }

        public static string GetTableName(string name)
        {
            List<string> stringList = new List<string>();
            stringList.AddRange((IEnumerable<string>)name.Split('.', StringSplitOptions.None));
            return stringList.Count > 1 ? stringList[stringList.Count - 2] : "Unk";
        }
    }
}
