### File structure

```bash
src/

├── common/                    # Common shared utilities, types, and constants 
├── config/                    # Configuration types and constants 
├── storage/                   # Key/value storage engine
│   ├── buffer/                # Buffer management logic for database pages
│   │   └── buffer_pool_manager
│   │   └── lru_k_replacer
│   ├── disk/                  # File storage logic
│   │   └── disk_manager
│   ├── heap/                  # Heap file manager 
│   ├── index/                 # [unimplemented] Table index 
│   ├── page/                  # Pages in memory 
│   │   ├── table_page         
│   │   ├── page               # Page trait definition 
│   │   └── record_id          
│   ├── tuple/                 # Table row data structure 
│   ├── engine                 # Storage engine trait definition
│   ├── simple                 # Serializes transactional access to storage engine 
│   └── tables                 # Storage engine that dispatches to heap file managers
├── types/                     # SQL types (also used in storage engine tests)
├── lib.rs                     # Project-wide library file
└── main.rs                    # Executable entry point for the project
```

#NU-CS339-Lab2
