import boto3

# Configuración de AWS Glue
glue_client = boto3.client('glue', region_name='us-east-1')  # Cambia la región si es necesario

# Nombre de la base de datos de Glue
database_name = 'rockie_database'

# Esquemas personalizados para algunas tablas
schema_t_students = [
    {"Name": "tenant_id", "Type": "string", "Comment": ""},
    {"Name": "student_id", "Type": "string", "Comment": ""},
    {"Name": "student_email", "Type": "string", "Comment": ""},
    {"Name": "fecha_ingreso", "Type": "string", "Comment": ""},
    {"Name": "nombre", "Type": "string", "Comment": ""},
    {"Name": "contraseña", "Type": "string", "Comment": ""},
    {"Name": "fecha_nacimiento", "Type": "string", "Comment": ""},
    {"Name": "genero", "Type": "string", "Comment": ""},
    {"Name": "telefono", "Type": "string", "Comment": ""},
    {"Name": "rockie_coins", "Type": "int", "Comment": ""},
    {"Name": "rockie_gems", "Type": "int", "Comment": ""},
    {"Name": "student_promos", "Type": "string", "Comment": ""}
]

schema_t_rockies =  [
  {
    "Name": "tenant_id",
    "Type": "string"
  },
  {
    "Name": "student_id",
    "Type": "string"
  },
  {
    "Name": "level",
    "Type": "int"
  },
  {
    "Name": "experience",
    "Type": "int"
  },
  {
    "Name": "evolution",
    "Type": "string"
  },
  {
    "Name": "rockie_name",
    "Type": "string"
  },
  {
    "Name": "arms_acc",
    "Type": "string"
  },
  {
    "Name": "head_acc",
    "Type": "string"
  },
  {
    "Name": "body_acc",
    "Type": "string"
  },
  {
    "Name": "face_acc",
    "Type": "string"
  },
  {
    "Name": "bg_acc",
    "Type": "string"
  },
  {
    "Name": "student_equipment",
    "Type": "string"
  }
]

schema_t_rewards = [
  {
    "Name": "tenant_id",
    "Type": "string"
  },
  {
    "Name": "student_id",
    "Type": "string"
  },
  {
    "Name": "reward_id",
    "Type": "string"
  },
  {
    "Name": "experience",
    "Type": "int"
  },
  {
    "Name": "activity_id",
    "Type": "string"
  },
  {
    "Name": "rockie_coins",
    "Type": "int"
  }
]

schema_t_promo = [
  {
    "Name": "tenant_id",
    "Type": "string"
  },
  {
    "Name": "product_id",
    "Type": "string"
  },
  {
    "Name": "price",
    "Type": "double"
  },
  {
    "Name": "image",
    "Type": "string"
  },
  {
    "Name": "product_brand",
    "Type": "string"
  },
  {
    "Name": "category",
    "Type": "string"
  },
  {
    "Name": "product_name",
    "Type": "string"
  }
]


schema_t_accesory = [
  {
    "Name": "tenant_id",
    "Type": "string"
  },
  {
    "Name": "product_id",
    "Type": "string"
  },
  {
    "Name": "price",
    "Type": "double"
  },
  {
    "Name": "image",
    "Type": "string"
  },
  {
    "Name": "category",
    "Type": "string"
  },
  {
    "Name": "product_name",
    "Type": "string"
  }
]
schema_t_activities = [
  {
    "Name": "tenant_id",
    "Type": "string"
  },
  {
    "Name": "activity_id",
    "Type": "string"
  },
  {
    "Name": "student_id",
    "Type": "string"
  },
  {
    "Name": "activity_type",
    "Type": "string"
  },
  {
    "Name": "creation_date",
    "Type": "string"
  },
  {
    "Name": "time",
    "Type": "int"
  }
]



# Lista de tablas a crear
tables = {
    'prod': ['t_rockies', 't_students', 't_rewards', 't_activities', 't_accesories', 't_promos'],
    'test': ['t_rockies', 't_students', 't_rewards', 't_activities', 't_accesories', 't_promos'],
    'dev': ['t_rockies', 't_students', 't_rewards', 't_activities', 't_accesories', 't_promos']
}

# Función para crear una tabla en AWS Glue
def create_table(stage, table_name):
    table_name_full = f"{stage}_{table_name}"
    
    # Determinar el esquema dependiendo del nombre de la tabla
    if table_name == 't_students':
        schema = schema_t_students
    elif table_name == 't_rockies':
        schema = schema_t_rockies
    elif table_name == 't_rewards':
        schema = schema_t_rewards
    elif table_name == 't_promos':
        schema = schema_t_promo
    elif table_name == 't_accesories':
        schema = schema_t_accesory
    elif table_name == 't_activities':
        schema = schema_t_activities
    else:
        print(f"Esquema no definido para la tabla {table_name_full}.")
    
    # Crear la definición de la tabla
    table_input = {
        'Name': table_name_full,
        'Description': f"Tabla {table_name_full} para el stage {stage}",
        'StorageDescriptor': {
            'Columns': schema,
            'Location': f"s3://ciencia-datos-bucket-rockie/{stage}/{table_name}/",  # Cambia el bucket y la ruta
            'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
            'SerdeInfo': {
                'Name': 'OpenCSVSerde',
                'SerializationLibrary': 'org.apache.hadoop.hive.serde2.OpenCSVSerde',
                'Parameters': {
                    'separatorChar': ','
                }
            }
        },
        'PartitionKeys': [],
        'TableType': 'EXTERNAL_TABLE'
    }
    
    # Crear la tabla en AWS Glue
    glue_client.create_table(
        DatabaseName=database_name,
        TableInput=table_input
    )
    print(f"Tabla {table_name_full} creada en el stage {stage}.")

# Crear todas las tablas para los diferentes stages
for stage in tables:
    for table in tables[stage]:
        create_table(stage, table)