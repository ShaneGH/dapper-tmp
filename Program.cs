using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
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

                var then = DateTime.Now;
                await conn.GetById<Employee, int>(1);
                Console.WriteLine("1: " + (DateTime.Now - then).TotalMilliseconds);

                then = DateTime.Now;
                var employees = await conn.GetById<Employee, int>(1);
                Console.WriteLine("2: " + (DateTime.Now - then).TotalMilliseconds);

                Console.WriteLine(JsonConvert.SerializeObject(employees));
            }

            Console.WriteLine("Hello World!");
        }
    }

    public static class DapperWrapper
    {
        public static async Task<List<TEntity>> GetById<TEntity, TId>(this IDbConnection conn, TId id)
            where TEntity : IEntity<TId>
        {
            var entityType = typeof(TEntity);
            var entityInfo = EntityInfoBuilderAttribute.GetEntityInfo<TEntity>();
            var sql = entityInfo.ValueTypes
                .Select(p => $"SELECT * FROM {p.TableName} WHERE {p.ParentEntityKey} = @id")
                .Append($"SELECT * FROM {entityType.Name} WHERE {entityInfo.PrimaryKeyName} = @id")
                .JoinStrings(";");

            using (var query = conn.QueryMultiple(sql, new { id }))
            {
                var data = await entityInfo.ValueTypes
                    .Select(x => x.LoadData(query))
                    .Traverse();

                var entities = (await query.ReadAsync<TEntity>()).ToList();
                foreach (var entity in entities)
                {
                    for (var i = 0; i < entityInfo.ValueTypes.Count; i++)
                    {
                        entityInfo.ValueTypes[i].ApplyCorrectData(
                            entity, 
                            data[i]);
                    }
                }

                return entities;
            }
        }

        public static string JoinStrings(this IEnumerable<string> strings, string separator)
        {
            return String.Join(separator, strings);
        }

        public static async Task<List<T>> Traverse<T>(this IEnumerable<Task<T>> values)
        {
            var results = new List<T>();
            foreach (var value in values)
            {
                results.Add(await value);
            }

            return results;
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
        public MyList<AddressZip> AddressZip { get; set; }
        int IEntity<int>.Id => EmployeeId;

        [EntityInfoBuilder]
        public static IEntityInfo<Employee> Create()
        {
            return EntityInfo.Create<Employee>(
                nameof(Employee), 
                nameof(EmployeeId),
                ValueType.Create<Employee, AddressCity, int>(nameof(dapper_messing.AddressCity), nameof(dapper_messing.AddressCity.EmployeeId), x => x.AddressCity),
                ValueType.Create<Employee, AddressZip, int>(nameof(dapper_messing.AddressZip), nameof(dapper_messing.AddressZip.EmployeeId), x => x.AddressZip));
        }
    }

    public class ValueType
    {
        public static IValueType<TEntity> Create<TEntity, TPropertyType, TPrimaryKey>(string tableName, string primaryKeyName, Expression<Func<TEntity, MyList<TPropertyType>>> property)
            where TEntity : IEntity<TPrimaryKey>
            where TPropertyType : ISubEntity<TPrimaryKey>
        {
            return new ValueTypeValues<TEntity, TPropertyType, TPrimaryKey>(tableName ,primaryKeyName, property);
        }

        private class ValueTypeValues<TEntity, TPropertyType, TPrimaryKey> : IValueType<TEntity>
            where TEntity : IEntity<TPrimaryKey>
            where TPropertyType : ISubEntity<TPrimaryKey>
        {
            public string TableName { get; }
            public string ParentEntityKey { get; }
            private readonly Action<TEntity, MyList<TPropertyType>> _setter;
            private readonly bool _keyIsValueType;

            public ValueTypeValues(string tableName, string parentEntityKey, Expression<Func<TEntity, MyList<TPropertyType>>> property)
                : this(tableName, parentEntityKey, BuildSetter(property))
            {
            }

            public ValueTypeValues(string tableName, string parentEntityKey, Action<TEntity, MyList<TPropertyType>> setter)
            {
                TableName = tableName;
                ParentEntityKey = parentEntityKey;
                _setter = setter;
                _keyIsValueType = typeof(TPrimaryKey).IsValueType;
            }

            private static Action<TEntity, MyList<TPropertyType>> BuildSetter(Expression<Func<TEntity, MyList<TPropertyType>>> property)
            {
                var newValue = Expression.Parameter(typeof(MyList<TPropertyType>));
                return Expression
                    .Lambda<Action<TEntity, MyList<TPropertyType>>>(
                        Expression.Assign(
                            property.Body,
                            newValue), property.Parameters[0], newValue)
                    .Compile();
            }

            public void ApplyCorrectData(TEntity entity, IEnumerable data)
            {
                var relevantData = data
                    .Cast<TPropertyType>()
                    .Where(_keyIsValueType ? (Func<TPropertyType, bool>)CompareValueType : CompareReferenceType);

                _setter(entity, new MyList<TPropertyType>(relevantData));

                bool CompareValueType(TPropertyType property) => property.EntityId.Equals(entity.Id);

                bool CompareReferenceType(TPropertyType property)
                {
                    if (property.EntityId == null && entity.Id == null)
                        return true;
                        
                    if (property.EntityId == null || entity.Id == null)
                        return false;

                    return property.EntityId.Equals(entity.Id);
                }
            }

            public async Task<IEnumerable> LoadData(SqlMapper.GridReader reader)
            {
                return await reader.ReadAsync<TPropertyType>();
            }
        }
    }

    public class EntityInfo
    {
        public static IEntityInfo<T> Create<T>(string tableName, string primaryKeyName, params IValueType<T>[] valueTypes)
        {
            return new EntityInfoValues<T>(tableName ,primaryKeyName, valueTypes.ToList().AsReadOnly());
        }

        private class EntityInfoValues<T> : IEntityInfo<T>
        {
            public string TableName { get; }

            public string PrimaryKeyName { get; }

            public ReadOnlyCollection<IValueType<T>> ValueTypes { get; }

            public EntityInfoValues(string tableName, string primaryKeyName, ReadOnlyCollection<IValueType<T>> valueTypes)
            {
                TableName = tableName;
                PrimaryKeyName = primaryKeyName;
                ValueTypes = valueTypes;
            }
        }
    }

    [System.AttributeUsage(System.AttributeTargets.Method, Inherited = false, AllowMultiple = false)]
    public sealed class EntityInfoBuilderAttribute : System.Attribute
    {
        private static ConcurrentDictionary<Type, object> InfoCache = new ConcurrentDictionary<Type, object>();

        public static IEntityInfo<TEntity> GetEntityInfo<TEntity>()
        {
            var forType = typeof(TEntity);
            if (InfoCache.TryGetValue(forType, out var value))
            {
                return (IEntityInfo<TEntity>)value;
            }

            var paramMethods = forType
                .GetMethods(BindingFlags.Public | BindingFlags.Static)
                .Where(x => x.GetCustomAttribute<EntityInfoBuilderAttribute>() != null)
                .ToList();

            if (paramMethods.Count == 0)
                throw new Exception($"Could not find builder for type {forType}. Builders must be public static methods.");
                
            if (paramMethods.Count != 1)
                throw new Exception($"More than one builder found for type {forType}");

            if (paramMethods[0].GetParameters().Length != 0)
                throw new Exception($"Builder for {forType} should not have any parameters.");

            if (!typeof(IEntityInfo<TEntity>).IsAssignableFrom(paramMethods[0].ReturnType))
                throw new Exception($"Builder for {forType} does not return {nameof(IEntityInfo<TEntity>)}.");

            value = paramMethods[0].Invoke(null, new object[0]);
            InfoCache.TryAdd(forType, value);
            
            return (IEntityInfo<TEntity>)value;
        }
    }

    public class OtherParameter
    {
        public string TableName { get; set; }
        public string ParentEntityKey { get; set; }
    }

    public interface IEntityInfo<TEntity>
    {
        string TableName { get; }
        string PrimaryKeyName { get; }
        ReadOnlyCollection<IValueType<TEntity>> ValueTypes { get; }
        
    }

    public interface IValueType<TEntity>
    {
        string TableName { get; }
        string ParentEntityKey { get; }
        Task<IEnumerable> LoadData(SqlMapper.GridReader reader);
        void ApplyCorrectData(TEntity entity, IEnumerable data);
    }

    public class MyList<T> : List<T>
    {
        public MyList(IEnumerable<T> values)
            : base(values)
        {
        }
    }

    public class AddressCity : EmployeeObject
    {
		public string Val { get; set; }
    }

    public class AddressZip : EmployeeObject
    {
		public string Val { get; set; }
    }

    public class EmployeeObject : DatedObject, ISubEntity<int>
    {
	    public int EmployeeId { get; set; }

        int ISubEntity<int>.EntityId => EmployeeId;
    }

    public class DatedObject
    {
	    public int EStart { get; set; }
	    public int EEnd { get; set; }
	    public int AStart { get; set; }
	    public int AEnd { get; set; }
    }
}
