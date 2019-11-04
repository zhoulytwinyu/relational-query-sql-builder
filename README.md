# relational-query-sql-builder

# Install
```
npm install zhoulytwinyu/relatioanl-query-sql-builder
```

# Usage
```
const SqlBuilder = require("relational-query-sql-builder");
let mySchema = {...}
let mySqlBuilder = new SqlBuilder(mySchema);
let sqlSet = mySqlBuilder.buildSQL(query);

let [sql,binds] = sqlSet[ENTITY_OF_INTEREST];
```

# Schema
1. The schema need to form an undirected acyclic graph
2. Every entity (table) must have an "__ID__" attribute
3. Reference to another entity must be named "__REF__XXXXX"
4. Because I don't know how to process CTE SQL, attributes and references must be (re)defined in schema.

```
{
  PATIENT:{
    CTE:`
      PATIENT AS (
      SELECT /*+ INLINE */
        PERSON_ID AS "__ID__",
        NAME_FIRST,
        NAME_MIDDLE,
        NAME_LAST,
        SEX_CODE.VALUE AS SEX,
        BIRTH_UNIX_TS,
        DECEASED_UNIX_TS
      FROM PERSON
        JOIN SEX_CODE USING(SEX_CD)
      )
    `,
    ATTRIBUTE: {
      __ID__:null,
      NAME_FIRST:null,
      NAME_MIDDLE:null,
      NAME_LAST:null,
      SEX:null,
      BIRTH_UNIX_TS:null,
      DECEASED_UNIX_TS:null
    }
  },
  
  PATIENT_NAME:{
    CTE:`
      PATIENT_NAME AS (
      SELECT /*+ INLINE */
        PERSON_NAME_ID AS "__ID__",
        PERSON_ID AS "__REF__PATIENT",
        NAME_FIRST,
        NAME_LAST,
        NAME_MIDDLE,
        NAME_TYPE_CODE.VALUE AS NAME_TYPE
      FROM PERSON
        JOIN PERSON_NAME USING(PERSON_ID)
      )
    `,
    REFERENCE:{
      PATIENT:null,
    },
    ATTRIBUTE:{
      __ID__:null,
      NAME_FIRST:null,
      NAME_MIDDLE:null,
      NAME_LAST:null,
      NAME_TYPE:null,
    }
  },
  
  PATIENT_MRN:{
    CTE:`
      PATIENT_MRN AS (
      SELECT /*+ INLINE */
        CHB_MRN_ID AS "__ID__",
        PERSON_ID AS "__REF__PATIENT",
        MRN,
        BEG_EFFECTIVE_UNIX_TS,
        END_EFFECTIVE_UNIX_TS
      FROM CHB_MRN
      )
    `,
    REFERENCE:{
      PATIENT:null,
    },
    ATTRIBUTE: {
      __ID__:null,
      MRN:null,
      BEG_EFFECTIVE_DT_TM:null,
      END_EFFECTIVE_DT_TM:null
    }
  },
  
  PATIENT_PHONE:{
    CTE:`
      PATIENT_PHONE AS (
      SELECT /*+ INLINE */
        PERSON_PHONE_ID AS "__ID__",
        PERSON_ID AS "__REF__PATIENT",
        PHONE_NUM,
        PHONE_TYPE_CODE.VALUE AS PHONE_TYPE,
        BEG_EFFECTIVE_UNIX_TS,
        END_EFFECTIVE_UNIX_TS
      FROM PERSON_PHONE
        JOIN PHONE_TYPE_CODE USING(PHONE_TYPE_CD)
      )
    `,
    REFERENCE:{
      PATIENT:null,
    },
    ATTRIBUTE:{
      __ID__:null,
      PHONE_NUM:null,
      PHONE_TYPE:null,
      BEG_EFFECTIVE_UNIX_TS:null,
      END_EFFECTIVE_UNIX_TS:null,
    }
  },
  
  PATIENT_ENCOUNTER:{
    CTE:`
      PATIENT_ENCOUNTER AS (
      SELECT /*+ INLINE */
        ENCNTR_ID AS "__ID__",
        PERSON_ID AS "__REF__PATIENT",
        ENCNTR_TYPE_CODE.VALUE AS ENCOUNTER_TYPE,
        ARRIVE_UNIX_TS,
        DEPART_UNIX_TS
      FROM ENCOUNTER
        JOIN ENCNTR_TYPE_CODE USING(ENCNTR_TYPE_CD)
      )
    `,
    REFERENCE:{
      PATIENT:null,
    },
    ATTRIBUTE:{
      __ID__:null,
      ENCOUNTER_TYPE:null,
      ARRIVE_UNIX_TS:null,
      DEPART_UNIX_TS:null,
    }
  },
  
  PATIENT_BED_ASSIGNMENT:{
    CTE:`
      PATIENT_BED_ASSIGNMENT AS (
      SELECT /*+ INLINE */
        ENCNTR_BED_SPACE_ID AS "__ID__",
        ENCNTR_ID AS "__REF__PATIENT_ENCOUNTER",
        BED_CD AS "__REF__BED",
        START_UNIX_TS,
        END_UNIX_TS  
      FROM ENCNTR_BED_SPACE
      )
    `,
    REFERENCE: {
      PATIENT_ENCOUNTER:null,
      BED:null,
    },
    ATTRIBUTE: {
      __ID__:null,
      START_UNIX_TS:null,
      END_UNIX_TS:null,
    }
  },
  
  PERSONEL:{
    CTE:`
      PERSONEL AS (
      SELECT /*+ INLINE */
        CHB_PRSNL_ID AS "__ID__",
        NAME_FIRST,
        NAME_MIDDLE,
        NAME_LAST,
        SEX_CODE.VALUE AS SEX,
        BIRTH_UNIX_TS,
        DECEASED_UNIX_TS
      FROM CHB_PRSNL
        JOIN PERSON USING(PERSON_ID)
        JOIN SEX_CODE USING(SEX_CD)
      )
    `,
    ATTRIBUTE: {
      __ID__:null,
      NAME_FIRST:null,
      NAME_MIDDLE:null,
      NAME_LAST:null,
      SEX:null,
      BIRTH_UNIX_TS:null,
      DECEASED_UNIX_TS:null,
    }
  },
  
  PERSONEL_NAME:{
    CTE:`
      PERSONEL_NAME AS (
      SELECT
        PERSON_NAME_ID AS "__ID__",
        CHB_PRSNL_ID AS "__REF__PERSONEL",
        NAME_FIRST,
        NAME_LAST,
        NAME_MIDDLE,
        NAME_TYPE_CODE.VALUE AS NAME_TYPE
      FROM CHB_PRSNL
        JOIN PERSON USING(PERSON_ID)
        JOIN PERSON_NAME USING(PERSON_ID)
      )
    `,
    REFERENCE:{
      PERSONEL:null,
    },
    ATTRIBUTE:{
      __ID__:null,
      NAME_FIRST:null,
      NAME_MIDDLE:null,
      NAME_LAST:null,
      NAME_TYPE:null,
    }
  },
  
  PERSONEL_PHONE:{
    CTE:`
      PERSONEL_PHONE AS (
      SELECT /*+ INLINE */
        PERSON_PHONE_ID AS "__ID__",
        PERSON_ID AS "__REF__PERSONEL",
        PHONE_NUM,
        PHONE_TYPE_CODE.VALUE AS PHONE_TYPE,
        BEG_EFFECTIVE_UNIX_TS,
        END_EFFECTIVE_UNIX_TS
      FROM PERSON_PHONE
        JOIN PHONE_TYPE_CODE USING(PHONE_TYPE_CD)
      )
    `,
    REFERENCE: {
      PERSONEL:null,
    },
    ATTRIBUTE:{
      __ID__:null,
      PHONE_NUM:null,
      PHONE_TYPE:null,
      BEG_EFFECTIVE_UNIX_TS:null,
      END_EFFECTIVE_UNIX_TS:null,
    }
  },
  
  PERSONEL_BED_ASSIGNMENT: {
    CTE:`
      PERSONEL_BED_ASSIGNMENT AS (
      SELECT /*+ INLINE */
        BED_ASSIGN_ID AS "__ID__",
        CHB_PRSNL_ID AS "__REF__PERSONEL",
        BED_CD AS "__REF__BED",
        ASSIGN_TYPE_CODE.VALUE AS ASSIGN_TYPE,
        START_UNIX_TS,
        END_UNIX_TS
      FROM CHB_TRK_BED_ASSIGN
        JOIN CHB_TRK_ASSIGN USING(ASSIGN_ID)
        JOIN ASSIGN_TYPE_CODE USING(ASSIGN_TYPE_CD)
      )
    `,
    REFERENCE: {
      PERSONEL:null,
      BED:null,
    },
    ATTRIBUTE: {
      __ID__:null,
      ASSIGN_TYPE:null,
      START_UNIX_TS:null,
      END_UNIX_TS:null,
    }
  },
  
  BED: {
    CTE:`
      BED AS (
      SELECT /*+ INLINE */
        BED_CD AS "__ID__",
        ROOM_CD AS "__REF__ROOM",
        VALUE AS NAME
      FROM BED_CODE
      )
    `,
    REFERENCE: {
      ROOM: null,
    },
    ATTRIBUTE:{
      __ID__:null,
      NAME:null,
    }
  },
  
  ROOM: {
    CTE:`
      ROOM AS (
      SELECT /*+ INLINE */
        ROOM_CD AS "__ID__",
        NURSE_UNIT_CD AS "__REF__NURSE_UNIT",
        VALUE AS NAME
      FROM ROOM_CODE
      )
    `,
    REFERENCE: {
      NURSE_UNIT: null,
    },
    ATTRIBUTE:{
      __ID__:null,
      NAME:null,
    }
  },
  
  NURSE_UNIT: {
    CTE:`
      NURSE_UNIT AS (
      SELECT /*+ INLINE */
        NURSE_UNIT_CD AS "__ID__",
        VALUE AS NAME
      FROM NURSE_UNIT_CODE
      )
    `,
    ATTRIBUTE:{
      __ID__:null,
      NAME:null,
    },
  },
  
  HEART_RATE: {
    CTE: `
      HEART_RATE AS (
      SELECT /*+ INLINE */
        PERSON_ID || DTUNIX "__ID__",
        PERSON_ID AS "__REF__PATIENT",
        HR_EKG AS VALUE,
        DTUNIX AS UNIX_TS
      FROM VITALS
      )
    `,
    REFERENCE:{
      PATIENT:null
    },
    ATTRIBUTE: {
      __ID__:null,
      VALUE:null,
      UNIX_TS:null,
    }
  }
}
```

# Query
The query, we call relational query, is a still loosely defined standard. It is designed to be easy to prepare, to understand and to compile into SQL.
1. Relational query should be a valid JSON, because it is transmitted via http.
2. Relational query contains "SELECT" field to specify what need to be retrived and "FILTER" field to specify filters.
3. Other restictions apply (TODO).

## Example
```
{
  "SELECT": {
    "PATIENT": {
      "NAME_LAST":0,
      "NAME_FIRST":0
    }
  },
  "FILTER": [
    {
      "PERSONEL":{
        "NAME_LAST":["=","KHEIR"]
      },
      "PATIENT":{
        "NAME_LAST":["=","WOODS"]
      }
    }
  ]
}
```
