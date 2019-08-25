using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Dapper;
using MySql.Data.MySqlClient;
using Newtonsoft.Json;

namespace dapper_messing
{
    class Program
    {
        static void Main(string[] args)
        {
            MMMain().Wait();
        }

        static async Task MMMain()
        {
            using (var conn = new MySqlConnection("Server=127.0.0.1;Uid=root;Pwd=root;Allow User Variables=True;Database=employeetest;"))
            {
                conn.Open();
                using (var query = conn.QueryMultiple("SELECT * FROM Employee WHERE EmployeeId = @id;SELECT * FROM AddressCity WHERE EmployeeId = @id;", new { id = 1 }))
                {
                    (await query.ReadAsync<Employee>()).ToList();
                    (await query.ReadAsync<AddressCity>()).ToList();
                }

                (await conn.QueryAsync<Employee>("SELECT * FROM Employee WHERE EmployeeId = @id", new { id = 1 })).ToList();

                var now = DateTime.Now;
                //var employees = (await conn.QueryAsync<Employee>("SELECT * FROM Employee WHERE EmployeeId = @id", new { id = 1 })).ToList();
                List<Employee> employees = conn.GetById<Employee, int>(1);

                Console.WriteLine((DateTime.Now - now).TotalMilliseconds);
                Console.WriteLine(JsonConvert.SerializeObject(employees));
            }

            Console.WriteLine("Hello World!");
        }
    }

    public static class DapperWrapper
    {
        public static List<TEntity> GetById<TEntity, TId>(this IDbConnection conn, TId id)
            where TEntity : IEntity<TId>
        {
            var subEntities = typeof(TEntity)
                .GetProperties()
                .Where(IsMyList)
                .ToList();

            var sql = subEntities
                .Select(ConvertEntityToSql)
                .Append(ConvertClassNameToSql(typeof(TEntity).Name))
                .JoinStrings(";");

            using (var query = conn.QueryMultiple(sql, new { id }))
            {
                var data = LoadData<TEntity, TId>(query, subEntities).ToList();
                var employees = (List<TEntity>)data[data.Count - 1];
                foreach (var employee in employees)
                {
                    for (var i = 0; i < subEntities.Count; i++)
                    {
                        foreach (var dataPoint in data[i])
                        {
                            if (employee.Id.Equals(((ISubEntity<TId>)dataPoint).EntityId))
                            {
                                subEntities[i].SetValue(employee, dataPoint);
                            }
                        }
                    }
                }

                return employees;
            }
        }

        public static IEnumerable<IEnumerable> LoadData<T, TId>(SqlMapper.GridReader multiQuery, List<PropertyInfo> properties)
        {
            var read = typeof(SqlMapper.GridReader)
                .GetMethods()
                .Single(x => 
                    x.Name == "Read" 
                    && x.GetGenericArguments().Length == 1
                    && x.GetParameters().Length == 1
                    && x.GetParameters()[0].ParameterType == typeof(bool));

            foreach (var prop in properties)
            {
                var propertyValues = read
                    .MakeGenericMethod(prop.PropertyType.GetGenericArguments()[0])
                    .Invoke(multiQuery, new object[] { true });

                yield return typeof(List<>)
                    .MakeGenericType(prop.PropertyType.GetGenericArguments()[0])
                    .GetConstructor(new [] { typeof(IEnumerable<>).MakeGenericType(prop.PropertyType.GetGenericArguments()[0]) })
                    .Invoke(new [] { propertyValues }) as IEnumerable;
            }

            var entities = read
                .MakeGenericMethod(typeof(T))
                .Invoke(multiQuery, new object[] { true });

            yield return typeof(List<>)
                .MakeGenericType(typeof(T))
                .GetConstructor(new [] { typeof(IEnumerable<>).MakeGenericType(typeof(T)) })
                .Invoke(new [] { entities }) as IEnumerable;
        }

        public static IEnumerable<object> Convert(this IEnumerable values)
        {
            foreach (var val in values) yield return val;
        }

        public static string JoinStrings(this IEnumerable<string> strings, string separator)
        {
            return String.Join(separator, strings);
        }

        static string ConvertClassNameToSql(string className)
        {
            className = new Regex("s$").Replace(className, "");
            return $"SELECT * FROM {className} WHERE EmployeeId = @id";
        }

        static string ConvertEntityToSql(PropertyInfo property)
        {
            return ConvertClassNameToSql(property.Name);
        }

        static bool IsMyList(PropertyInfo property)
        {
            return property.PropertyType.IsGenericType
                && property.PropertyType.GetGenericTypeDefinition() == typeof(MyList<>);
        }
    }

    public interface IEntity<TId>
    {
        TId Id { get; }
    }

    public interface ISubEntity<TId>
    {
        TId EntityId { get; }
    }

    public class Employee : IEntity<int>
    {
        public int EmployeeId { get; set; }
        public MyList<AddressCity> AddressCity { get; set; }
        int IEntity<int>.Id => EmployeeId;
    }

    public class MyList<T> : List<T>
    {
        public MyList(IEnumerable<T> values)
            : base(values)
        {
        }
    }

    public class AddressCity : ISubEntity<int>
    {
	    public int EmployeeId { get; set; }
		public string Val { get; set; }
	    public int EStart { get; set; }
	    public int EEnd { get; set; }
	    public int AStart { get; set; }
	    public int AEnd { get; set; }

        int ISubEntity<int>.EntityId => EmployeeId;
    }
}
