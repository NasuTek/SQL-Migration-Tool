using IniParser.Model;
using Microsoft.Data.SqlClient;
using Microsoft.SqlServer.Dac;
using Serilog;

namespace NasuTek.MigrationTool;

internal class Program
{
    private static IniData IniFile { get; set; } = null!;

    public static void Main(string[] args)
    {
        IniFile = new IniParser.FileIniDataParser().ReadFile("Migration.ini");

        Log.Logger = new LoggerConfiguration()
            .WriteTo.Console()
            .WriteTo.File($"MigrationLog-{DateTime.Now:yyyyMMdd}.log")
            .CreateLogger();

        Log.Information("{appName} Version: {appVersion} Migration", IniFile["Version"]["AppName"], IniFile["Version"]["Version"]);

        foreach (var ps in IniFile["PreScripts"])
        {
            foreach (var dir in IniFile[$"PreScripts.{ps.Value}"])
            {
                RunScripts(ps.Value, dir.Value);
            }
        }

        foreach (var dir in IniFile["DacPacs"])
        {
            SyncDbs(dir.Value);
        }

        foreach (var ps in IniFile["PostScripts"])
        {
            foreach (var dir in IniFile[$"PostScripts.{ps.Value}"])
            {
                RunScripts(ps.Value, dir.Value);
            }
        }
    }

    private static void SyncDbs(string dacPacsPath)
    {
        Log.Information("-- Running dacpacs in {directoryName}...", dacPacsPath);
        try
        {
            var filesInDirectory = Directory.GetFiles(dacPacsPath);
            Log.Information("Database DACPACs found - {filesInDirectory}", filesInDirectory.Length);

            foreach (var dacPacPath in filesInDirectory)
            {
                try
                {
                    var dbName = Path.GetFileNameWithoutExtension(dacPacPath);
                    Log.Information("** Connecting to the {dbName} database...", dbName);
                    var services = new DacServices(IniFile["ConnectionStrings"]["0"]);
                    services.Message += (sender, args) => { Log.Information(args.Message.Message); };

                    var package = DacPackage.Load(dacPacPath);

                    Log.Information("** Upgrading {dbName} database...", dbName);
                    services.Deploy(package, dbName, true, new DacDeployOptions()
                    {
                        CompareUsingTargetCollation = true,
                        IgnoreColumnCollation = true,
                        DropObjectsNotInSource = true,
                        BlockOnPossibleDataLoss = false,
                        AllowDropBlockingAssemblies = true,
                        GenerateSmartDefaults = true,
                        DoNotDropObjectTypes = new[]
                        {
                            ObjectType.Users,
                            ObjectType.DatabaseTriggers,
                            ObjectType.ServerTriggers,
                            ObjectType.Credentials,
                            ObjectType.DatabaseOptions,
                            ObjectType.DatabaseRoles,
                            ObjectType.Permissions
                        }
                    });
                    Log.Information("** Database {dbName} upgrade complete...", dbName);
                }
                catch (Exception ex)
                {
                    Log.Error(ex, "DACPAC Upgrade Error");
                }
            }
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Error reading directory for dacpacs");
        }

        Log.Information("-- Running dacpacs in {directoryName} complete...", dacPacsPath);
    }

    private static void RunScripts(string databaseName, string directoryName)
    {
        Log.Information("-- Running {directoryName} scripts...", directoryName);
        try
        {
            var filesInDirectory = Directory.GetFiles(directoryName);

            Log.Information("Scripts found - {filesInDirectory}", filesInDirectory.Length);

            foreach (var file in filesInDirectory)
            {
                try
                {
                    using var sqlConn = new SqlConnection();
                    sqlConn.ConnectionString = $"{IniFile["ConnectionStrings"]["0"]};Database={databaseName}";
                    sqlConn.Open();

                    Log.Information("Running {file}...", Path.GetFileNameWithoutExtension(file));

                    new SqlCommand(File.ReadAllText(file), sqlConn) { CommandTimeout = 900 }.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    Log.Error(ex, "Error executing SQL Script on Migration");
                }
            }
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Error reading directory for scripts");
        }

        Log.Information("-- Running {directoryName} scripts complete...", directoryName);
    }
}