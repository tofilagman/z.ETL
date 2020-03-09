using z.ETL.ConnectionManager;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;

namespace z.ETL.Helper
{

    public static class Config
    {
        public class ConnectionDetails<TConnectionString, TConnectionManager>
        where TConnectionString : IDbConnectionString, new()
        where TConnectionManager : IConnectionManager, new()
        {
            public string ConnectionStringName { get; set; }
            public ConnectionDetails(string connectionStringName)
            {
                this.ConnectionStringName = connectionStringName;
            }
            public string RawConnectionString(string section)
               => Config.DefaultConfigFile.GetSection(section)[ConnectionStringName];
            public TConnectionString ConnectionString(string section)
                => new TConnectionString() { Value = RawConnectionString(section) };  
        }
          
        static IConfigurationRoot _defaultConfigFile;
        public static IConfigurationRoot DefaultConfigFile
        {
            get
            {
                if (_defaultConfigFile == null)
                {
                    var envvar = Environment.GetEnvironmentVariable("ETLBoxConfig");
                    var path = string.IsNullOrWhiteSpace(envvar) ? $"default.config.json" : envvar;
                    Load(path);
                }
                return _defaultConfigFile;
            }
            set
            {
                _defaultConfigFile = value;
            }
        }

        public static void Load(string configFile)
        {
            DefaultConfigFile = new ConfigurationBuilder()
                    .AddJsonFile(configFile)
                    .Build();
        }

    }
}
