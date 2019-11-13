function memoize_one(func) {
  let memo = {
    args:undefined,
    result:undefined
  };
  return function(...args) {
    if (memo.args===undefined) {
      memo.result = func(...args);
      memo.args = args;
      return memo.result;
    }
    let hit = true;
    if (memo.args.length===args.length) {
      for (let i=0; i<args.length; i++) {
        if (args[i]===memo.args[i]) {
          continue;
        }
        else {
          hit=false;
          break;
        }
      }
    }
    if (! hit) {
      memo.result = func(...args);
      memo.args = args;
    }
    return memo.result;
  };
}

class SqlBuilder {
  constructor(schema){
    if (this._isValidSchema(schema)) {
      this.schema = schema;
    }
    else {
      throw new SyntaxError("Schema not valid");
    }
    // setup memoization
    this._getUndirectedGraph = memoize_one( this._getUndirectedGraph.bind(this) );
    this._getFilterEntities = memoize_one( this._getFilterEntities.bind(this) );
    this._generateFilterStatement = memoize_one( this._generateFilterStatement.bind(this) );
  }
  
  buildSQL(query) {
    if (!this._isValidQuery(query)) {
      throw new SyntaxError("Query not valid");
    }
    let ret = {};
    let querySelect = query.select;
    for (let entity of Object.keys(querySelect)) {
      ret[entity] = this._generateSQL(entity,query);
    }
    return ret;
  }
  
  // Helper functions
  _isValidSchema(schema){ // should be converted to static method
    let g = null;
    try {
      g = this._getUndirectedGraph(schema);
    } catch (error) {
      switch (error.name) {
        case "SyntaxError":
          return false;
      }
    }
    let acyclicity = this._isConnectedUndirectedAcyclicGraph(g);
    if (! acyclicity) {
      return false;
    }
    return true;
  }

  _getUndirectedGraph(schema) { // should be converted to static method
    let g = this._getGraph(schema);
    let ug = {};
    for (let e of Object.keys(schema)) {
      ug[e] = {};
    }
    for ( let [e,eLink] of Object.entries(g) ){
      for ( let refE of Object.keys(eLink) ) {
        if (e in g[refE]) {
          throw new SyntaxError("Schema cannot be converted to undirected graph");
        }
        ug[e][refE] = null;
        ug[refE][e] = null;
      }
    }
    return ug;
  }

  _getGraph(schema){ // should be converted to static method
    let g = {};
    for (let n of Object.keys(schema)) {
      g[n] = {};
    }
    for (let [entity,config] of Object.entries(schema)) {
      let ref = config["references"] || {};
      for (let refEntity of Object.keys(ref)) {
        if (!(refEntity in g)) {
          throw new SyntaxError("Schema entity tries to reference an non-existent entity");
        }
        g[entity][refEntity] = null;
      }
    }
    return g;
  }

  _isConnectedUndirectedAcyclicGraph(graph) { // should be converted to static method
    if (Object.keys(graph).length<=1) {
      return true;
    }
    let visitedGraph = {};
    // To store depth-first search nodes
    // The graph traversal starts with node `n`
    //  and visits all connected nodes
    let n = Object.keys(graph)[0];
    let dfsStack =[n];
    while (dfsStack.length !== 0) {
      // The popped node is what is being visited
      let nn = dfsStack.pop();
      // If it has been visited, there must be a loop
      if ( nn in visitedGraph ) {
        return false;
      }
      visitedGraph[nn] = {};
      for (let nnn of Object.keys(graph[nn])) {
        // Skip edges that have been traversed 
        if (nnn in visitedGraph &&
            nn in visitedGraph[nnn]) {
          continue;
        }
        visitedGraph[nn][nnn] = null;
        // Traverse to the connected node
        dfsStack.push(nnn);
      }
    }
    // Not a connected graph
    if (Object.keys(graph).length !== Object.keys(visitedGraph).length) {
      return false;
    }
    return true;
  }
  
  _isValidQuery(query){
    /*
     * query must be an object containing proper "select"
     * "filter" is optional, and by default will be set up null
     * as downstream methods expect either a null or a proper filter
     */
    if (typeof query !== 'object'
        || query === null
        || !("select" in query)
        ){
      return false;
    }
    if (this._isValidQuerySelect(query.select)
        && this._isValidQueryFilter(query.filter)
        ) {
      return true;
    }
    return false;
  }
  
  _isValidQuerySelect(querySelect) {
    let {schema} = this;
    // Entities are defined in schema
    for (let e of Object.keys(querySelect)) {
      if ( !(e in schema) ){
        return false;
      }
    }
    // Entities attributes are defined in schema
    for (let e of Object.keys(querySelect)) {
      let definedAttrs = schema[e].attributes;
      let queryAttrs = Object.keys(querySelect[e]);
      for (let a of queryAttrs) {
        if ( !(a in definedAttrs) ){
          return false;
        }
      }
    }
    return true;
  }

  _isValidQueryFilter(queryFilter) {
    if (queryFilter === undefined 
        || queryFilter === null
        ) {
      return true;
    }
    let {schema} = this;
    let dfsStack = [queryFilter];
    // check filter structure
    while (dfsStack.length!==0 ) {
      let fn = dfsStack.pop();
      if ( !this._isValidFilterNode(fn) ) {
        return false;
      }
      if ("filters" in fn) {
        for (let n of fn.filters) {
          dfsStack.push(n);
        }
      }
    }
    // check filter entity attribute against schema
    let variables = this._getFilterVariables(queryFilter);
    for (let v of variables) {
      if ( !this._isValidFilterVariable(v) ) {
        return false;
      }
    }
    return true;
  }
  
  _isValidFilterNode(filterNode) {
    if (typeof filterNode === "object"
        && filterNode !== null
        && "op" in filterNode
        && filterNode.op in this.constructor.FILTER_OPERATOR
        ){
      let valid = this.constructor.FILTER_OPERATOR[filterNode.op].isValid(filterNode);
      if (valid) {
        return true;
      }
    }
    return false;
  }

  _isValidFilterVariable(filterVariable) {
    if (filterVariable === null
        || typeof filterVariable === "string"
        || typeof filterVariable === "number"
        || this._isValidEntityAttribute(filterVariable)
        ) {
      return true;
    }
    return false;
  }
  
  _isValidEntityAttribute(filterVariable) {
    let {schema} = this;
    if (filterVariable !== null
        && typeof filterVariable === "object"
        && typeof filterVariable.entity === "string"
        && typeof filterVariable.attribute === "string"
        && (filterVariable.entity in this.schema)
        && (filterVariable.attribute in this.schema[filterVariable.entity].attributes)
        ) {
      return true;
    }
    return false;
  }

  _getFilterVariables(queryFilter) {
    if (queryFilter === null) {
      return [];
    }
    let dfsStack = [queryFilter];
    let ret = [];
    while (dfsStack.length !== 0){
      let fNode = dfsStack.pop();
      if ("filters" in fNode) {
        dfsStack.push(...fNode.filters);
      }
      if ("variables" in fNode){
        ret.push(...fNode.variables);
      }
    }
    return ret;
  }
  
  _getFilterEntities(queryFilter) {
    let ret = [];
    for (let v of this._getFilterVariables(queryFilter)) {
      if (typeof v === "object"
          && v !== null
          ) {
        ret.push(v.entity);
      }
    }
    return [...new Set(ret)];
  }
  
  _generateSQL(entity,query) {
    let cteStatement = this._generateCTEStatement(entity,query);
    let selectStatement = this._generateSelectStatement(entity,query);
    let fromStatement = this._generateFromStatement(entity,query);
    let [whereStatement,binds] = this._generateWhereStatement(entity,query);
    return [
      `${cteStatement} ${selectStatement} ${fromStatement} ${whereStatement}`,
      binds
    ];
  }

  _generateCTEStatement(entity,query) {
    let {schema} = this;
    let querySelect = query.select;
    let queryFilter = query.filter;
    let statements = [];
    let includeCTE = {};
    includeCTE[entity] = null;
    let entities = this._getFilterEntities(queryFilter);
    let joinOrder = this._resolveJoinOrder(entity,entities);
    for (let joinPath of joinOrder) {
      for (let e of joinPath){
        includeCTE[e] = null;
      }
    }
    for ( let e of Object.keys(includeCTE) ) {
      statements.push(schema[e].cte);
    }
    return "WITH "+statements.join(',');
  }

  _generateSelectStatement(entity,query) {
    let {schema} = this;
    let querySelect = query.select;
    let queryAttrs = {...querySelect[entity]};
    queryAttrs["__ID__"] = null;
    if ("references" in schema[entity]) {
      for ( let refEntity of Object.keys(schema[entity].references) ) {
        if (refEntity in querySelect) {
          queryAttrs[`__REF__${refEntity}`] = null;
        }
      }
    }
    let statement = Object.keys(queryAttrs)
      .map( (attr)=>`"${entity}"."${attr}"` )
      .join( ',' );
    return `SELECT ${statement}`;
  }
  
  _generateFromStatement(entity,query) {
    return `FROM "${entity}"`;
  }

  _generateWhereStatement(entity,query) {
    if (!("filter" in query)
        || query.filter === null
        ) {
      return ["",[]];
    }
    let {schema} = this;
    let queryFilter = query.filter;
    let entities = this._getFilterEntities(queryFilter);
    let joinStatement = this._generateJoinStatement(entity,entities);
    let [filterStatement,binds] = this._generateFilterStatement(queryFilter);
    return [
      `WHERE "${entity}"."__ID__" IN (SELECT "${entity}"."__ID__" FROM ${joinStatement} WHERE ${filterStatement})`,
      binds
    ];
  }

  _generateJoinStatement(entity1,entities) {
    // Assuming entities joinable
    let {schema} = this;
    let joinStatement = [entity1];
    let joinOrder = this._resolveJoinOrder(entity1,entities);
    for (let joinPath of joinOrder){ 
      for (let i=0,j=1; j<joinPath.length; i++,j++) {
        let e1 = joinPath[i];
        let e2 = joinPath[j];
        let s = `JOIN "${e2}" ON `+this._generateAdjacentJoinOnStatement(e1,e2);
        joinStatement.push(s);
      }
    }
    return joinStatement.join(' ');
  }
  
  _resolveJoinOrder(entity1,entities){
    let joinOrder = [];
    let joined = {};
    for (let entity2 of entities) {
      let joinPath = this._resolveJoinPath(entity1,entity2);
      // Remove joined entities from path
      for (let i=0,j=1; j<joinPath.length; i++,j++) {
        let e2 = joinPath[j];
        if (e2 in joined) {
          continue;
        }
        else {
          joinPath = joinPath.slice(i);
          break;
        }
      }
      for (let i=1; i<joinPath.length; i++) {
        joined[joinPath[i]] = null;
      }
      joinOrder.push(joinPath);
    }
    return joinOrder;
  }

  _resolveJoinPath(entity1,entity2){
    // Assuming both entities are found in schema
    let {schema} = this;
    let graph = this._getUndirectedGraph(schema);
    let joinPathStack = [[]];
    let dfsStack = [entity1];
    let visitedGraph = {};
    let joinPath = null;
    while ( dfsStack.length !== 0 ){
      let n = dfsStack.pop();
      joinPath = [...joinPathStack.pop(),n];
      visitedGraph[n] = 1;
      // Destination reached
      if (n === entity2) {
        break;
      }
      for (let nn of Object.keys(graph[n])) {
        if (nn in visitedGraph) {
          continue;
        }
        dfsStack.push(nn);
        joinPathStack.push(joinPath);
      }
    }
    return joinPath;
  }
  
  _generateAdjacentJoinOnStatement(entity1,entity2) {
    // Internal method, assuming entity1, entity2 both in schema and adjacent
    let {schema} = this;
    let joinOnStatement = null;
    // Either entity1 references entity2
    if ("references" in schema[entity1] &&
        entity2 in schema[entity1].references) {
      joinOnStatement = `"${entity1}"."__REF__${entity2}"="${entity2}"."__ID__"`;
    }
    // Or entity2 references entity1
    else if ("references" in schema[entity2] &&
        entity1 in schema[entity2].references) {
      joinOnStatement = `"${entity1}"."__ID__"="${entity2}"."__REF__${entity1}"`;
    }
    else {
      throw new Error("Not adjacent entities");
    }
    return joinOnStatement;
  }

  _generateFilterStatement(filterNode) {
    // Deep copy filter node
    // because I don't want to modify input. Being pure.
    let filterNodeCopy = {...filterNode};
    let dfsStack = [filterNodeCopy];
    while (dfsStack.length !== 0) {
      let fn = dfsStack.pop();
      if ("filters" in fn) {
        fn.filters = fn.filters.map( n=>({...n}) );
        for (let n of fn.filters) {
          dfsStack.push(n);
        }
      }
    }
    // Prepare pre-order stack
    let preOrderStack = [];
    dfsStack = [filterNodeCopy];
    while (dfsStack.length !== 0) {
      let fn = dfsStack.pop();
      preOrderStack.push(fn);
      if ("filters" in fn){
        for (let n of fn.filters) {
          dfsStack.push(n);
        }
      }
    }
    // Work out the result
    let bindsCount = 1;
    while (preOrderStack.length !== 0) {
      let fn = preOrderStack.pop();
      let [statement,binds] = this.constructor.FILTER_OPERATOR[fn.op].generateStatement(fn,bindsCount);
      fn.statement = statement;
      fn.binds = binds;
      bindsCount += binds.length;
    }
    let statement = filterNodeCopy.statement;
    let binds = filterNodeCopy.binds;
    return [statement,binds];
  }
}

SqlBuilder.FILTER_OPERATOR = {
  "NOT": {
    isValid: (fn) => {
      if (Array.isArray(fn.filters)
          && fn.filters.length === 1
          && fn.variables ===undefined
          ) {
        return true;
      }
      return false;
    },
    generateStatement: (fn)=> {
      let {filters} = fn;
      let statement = "NOT "+fn.filters[0].statement;
      let binds = fn.filters[0].binds;
      return [statement,binds];
    }
  },
  "AND": {
    isValid: (fn) => {
      if (Array.isArray(fn.filters)
          && fn.filters.length >= 2
          && fn.variables === undefined
          ) {
        return true;
      }
      return false;
    },
    generateStatement: (fn)=> {
      let {filters} = fn;
      let statement = fn.filters.map( n=>n.statement ).join(" AND ");
      let binds = [].concat(...fn.filters.map( n=>n.binds ));
      return [statement,binds];
    }
  },
  "OR": {
    isValid: (fn) => {
      if (Array.isArray(fn.filters)
          && fn.filters.length >= 2
          && fn.variables === undefined
          ) {
        return true;
      }
      return false;
    },
    generateStatement: (fn)=> {
      let statement = fn.filters.map( n=>n.statement ).join(" OR ");
      let binds = [].concat(...fn.filters.map( n=>n.binds ));
      return [statement,binds];
    }
  },
  "=": {
    isValid: (fn) => {
      if (fn.filters === undefined
          && Array.isArray(fn.variables)
          && fn.variables.length === 2
          ) {
        return true;
      }
      return false;
    },
    generateStatement: (fn,bindsCount)=> {
      let binds = [];
      let inStatementVariables = [];
      for (let v of fn.variables) {
        if (v===null
            || typeof v === "string"
            || typeof v === "number"
            ){
          binds.push(v);
          inStatementVariables.push(`:${bindsCount}`);
        }
        else { // v is {entity:"xxx",attribute:"xxxxx"}
          inStatementVariables.push(`"${v.entity}"."${v.attribute}"`);
        }
      }
      let statement = inStatementVariables.join("=");
      return [statement,binds];
    }
  },
  "<": {
    isValid: (fn) => {
      if (fn.filters === undefined
          && Array.isArray(fn.variables)
          && fn.variables.length === 2
          ) {
        return true;
      }
      return false;
    },
    generateStatement: (fn,bindsCount)=> {
      let binds = [];
      let inStatementVariables = [];
      for (let v of fn.variables) {
        if (v===null
            || typeof v === "string"
            || typeof v === "number"
            ){
          binds.push(v);
          inStatementVariables.push(`:${bindsCount}`);
        }
        else { // v is {entity:"xxx",attribute:"xxxxx"}
          inStatementVariables.push(`"${v.entity}"."${v.attribute}"`);
        }
      }
      let statement = inStatementVariables.join("<");
      return [statement,binds];
    }
  },
  ">":{
    isValid: (fn) => {
      if (fn.filters === undefined
          && Array.isArray(fn.variables)
          && fn.variables.length === 2
          ) {
        return true;
      }
      return false;
    },
    generateStatement: (fn,bindsCount)=> {
      let binds = [];
      let inStatementVariables = [];
      for (let v of fn.variables) {
        if (v===null
            || typeof v === "string"
            || typeof v === "number"
            ){
          binds.push(v);
          inStatementVariables.push(`:${bindsCount}`);
        }
        else { // v is {entity:"xxx",attribute:"xxxxx"}
          inStatementVariables.push(`"${v.entity}"."${v.attribute}"`);
        }
      }
      let statement = inStatementVariables.join(">");
      return [statement,binds];
    }
  },
  "<=":{
    isValid: (fn) => {
      if (fn.filters === undefined
          && Array.isArray(fn.variables)
          && fn.variables.length === 2
          ) {
        return true;
      }
      return false;
    },
    generateStatement: (fn,bindsCount)=> {
      let binds = [];
      let inStatementVariables = [];
      for (let v of fn.variables) {
        if (v===null
            || typeof v === "string"
            || typeof v === "number"
            ){
          binds.push(v);
          inStatementVariables.push(`:${bindsCount}`);
        }
        else { // v is {entity:"xxx",attribute:"xxxxx"}
          inStatementVariables.push(`"${v.entity}"."${v.attribute}"`);
        }
      }
      let statement = inStatementVariables.join("<=");
      return [statement,binds];
    }
  },
  ">=":{
    isValid: (fn) => {
      if (fn.filters === undefined
          && Array.isArray(fn.variables)
          && fn.variables.length === 2
          ) {
        return true;
      }
      return false;
    },
    generateStatement: (fn,bindsCount)=> {
      let binds = [];
      let inStatementVariables = [];
      for (let v of fn.variables) {
        if (v===null
            || typeof v === "string"
            || typeof v === "number"
            ){
          binds.push(v);
          inStatementVariables.push(`:${bindsCount}`);
        }
        else { // v is {entity:"xxx",attribute:"xxxxx"}
          inStatementVariables.push(`"${v.entity}"."${v.attribute}"`);
        }
      }
      let statement = inStatementVariables.join(">=");
      return [statement,binds];
    }
  },
  "!=":{
    isValid: (fn) => {
      if (fn.filters === undefined
          && Array.isArray(fn.variables)
          && fn.variables.length === 2
          ) {
        return true;
      }
      return false;
    },
    generateStatement: (fn,bindsCount)=> {
      let binds = [];
      let inStatementVariables = [];
      for (let v of fn.variables) {
        if (v===null
            || typeof v === "string"
            || typeof v === "number"
            ){
          binds.push(v);
          inStatementVariables.push(`:${bindsCount}`);
        }
        else { // v is {entity:"xxx",attribute:"xxxxx"}
          inStatementVariables.push(`"${v.entity}"."${v.attribute}"`);
        }
      }
      let statement = inStatementVariables.join("!=");
      return [statement,binds];
    }
  },
  "LIKE": {
    isValid: (fn) => {
      if (fn.filters === undefined
          && Array.isArray(fn.variables)
          && fn.variables.length === 2
          ) {
        return true;
      }
      return false;
    },
    generateStatement: (fn,bindsCount)=> {
      let binds = [];
      let inStatementVariables = [];
      for (let v of fn.variables) {
        if (v===null
            || typeof v === "string"
            || typeof v === "number"
            ){
          binds.push(v);
          inStatementVariables.push(`:${bindsCount}`);
        }
        else { // v is {entity:"xxx",attribute:"xxxxx"}
          inStatementVariables.push(`"${v.entity}"."${v.attribute}"`);
        }
      }
      let statement = inStatementVariables.join("LIKE");
      return [statement,binds];
    }
  },
  "NOT LIKE": {
    isValid: (fn) => {
      if (fn.filters === undefined
          && Array.isArray(fn.variables)
          && fn.variables.length === 2
          ) {
        return true;
      }
      return false;
    },
    generateStatement: (fn,bindsCount)=> {
      let binds = [];
      let inStatementVariables = [];
      for (let v of fn.variables) {
        if (v===null
            || typeof v === "string"
            || typeof v === "number"
            ){
          binds.push(v);
          inStatementVariables.push(`:${bindsCount}`);
        }
        else { // v is {entity:"xxx",attribute:"xxxxx"}
          inStatementVariables.push(`"${v.entity}"."${v.attribute}"`);
        }
      }
      let statement = inStatementVariables.join("NOT LIKE");
      return [statement,binds];
    }
  },
  "IS": {
    isValid: (fn) => {
      if (fn.filters === undefined
          && Array.isArray(fn.variables)
          && fn.variables.length === 2
          ) {
        return true;
      }
      return false;
    },
    generateStatement: (fn,bindsCount)=> {
      let binds = [];
      let inStatementVariables = [];
      for (let v of fn.variables) {
        if (v===null
            || typeof v === "string"
            || typeof v === "number"
            ){
          binds.push(v);
          inStatementVariables.push(`:${bindsCount}`);
        }
        else { // v is {entity:"xxx",attribute:"xxxxx"}
          inStatementVariables.push(`"${v.entity}"."${v.attribute}"`);
        }
      }
      let statement = inStatementVariables.join("IS");
      return [statement,binds];
    }
  },
  "IS NOT": {
    isValid: (fn) => {
      if (fn.filters === undefined
          && Array.isArray(fn.variables)
          && fn.variables.length === 2
          ) {
        return true;
      }
      return false;
    },
    generateStatement: (fn,bindsCount)=> {
      let binds = [];
      let inStatementVariables = [];
      for (let v of fn.variables) {
        if (v===null
            || typeof v === "string"
            || typeof v === "number"
            ){
          binds.push(v);
          inStatementVariables.push(`:${bindsCount}`);
        }
        else { // v is {entity:"xxx",attribute:"xxxxx"}
          inStatementVariables.push(`"${v.entity}"."${v.attribute}"`);
        }
      }
      let statement = inStatementVariables.join("IS NOT");
      return [statement,binds];
    }
  },
  "BETWEEN": {
    isValid: (fn) => {
      if (fn.filters === undefined
          && Array.isArray(fn.variables)
          && fn.variables.length === 2
          ) {
        return true;
      }
      return false;
    },
    generateStatement: (fn,bindsCount)=> {
      let binds = [];
      let inStatementVariables = [];
      for (let v of fn.variables) {
        if (v===null
            || typeof v === "string"
            || typeof v === "number"
            ){
          binds.push(v);
          inStatementVariables.push(`:${bindsCount}`);
        }
        else { // v is {entity:"xxx",attribute:"xxxxx"}
          inStatementVariables.push(`"${v.entity}"."${v.attribute}"`);
        }
      }
      let statement = `${inStatementVariables[0]} BETWEEN ${inStatementVariables[1]} AND ${inStatementVariables[2]}`;
      return [statement,binds];
    }
  },
  "NOT BETWEEN": {
    isValid: (fn) => {
      if (fn.filters === undefined
          && Array.isArray(fn.variables)
          && fn.variables.length === 2
          ) {
        return true;
      }
      return false;
    },
    generateStatement: (fn,bindsCount)=> {
      let binds = [];
      let inStatementVariables = [];
      for (let v of fn.variables) {
        if (v===null
            || typeof v === "string"
            || typeof v === "number"
            ){
          binds.push(v);
          inStatementVariables.push(`:${bindsCount}`);
        }
        else { // v is {entity:"xxx",attribute:"xxxxx"}
          inStatementVariables.push(`"${v.entity}"."${v.attribute}"`);
        }
      }
      let statement = `${inStatementVariables[0]} NOT BETWEEN ${inStatementVariables[1]} AND ${inStatementVariables[2]}`;
      return [statement,binds];
    }
  },
  "IN": {
    isValid: (fn) => {
      if (fn.filters === undefined
          && Array.isArray(fn.variables)
          && fn.variables.length === 2
          ) {
        return true;
      }
      return false;
    },
    generateStatement: (fn,bindsCount)=> {
      let binds = [];
      let inStatementVariables = [];
      for (let v of fn.variables) {
        if (v===null
            || typeof v === "string"
            || typeof v === "number"
            ){
          binds.push(v);
          inStatementVariables.push(`:${bindsCount}`);
        }
        else { // v is {entity:"xxx",attribute:"xxxxx"}
          inStatementVariables.push(`"${v.entity}"."${v.attribute}"`);
        }
      }
      let statement = `${inStatementVariables[0]} IN (${ inStatementVariables.slice(1).join(',') })`;
      return [statement,binds];
    }
  },
  "NOT IN": {
    isValid: (fn) => {
      if (fn.filters === undefined
          && Array.isArray(fn.variables)
          && fn.variables.length === 2
          ) {
        return true;
      }
      return false;
    },
    generateStatement: (fn,bindsCount)=> {
      let binds = [];
      let inStatementVariables = [];
      for (let v of fn.variables) {
        if (v===null
            || typeof v === "string"
            || typeof v === "number"
            ){
          binds.push(v);
          inStatementVariables.push(`:${bindsCount}`);
        }
        else { // v is {entity:"xxx",attribute:"xxxxx"}
          inStatementVariables.push(`"${v.entity}"."${v.attribute}"`);
        }
      }
      let statement = `${inStatementVariables[0]} NOT IN (${ inStatementVariables.slice(1).join(',') })`;
      return [statement,binds];
    }
  },
}

module.exports = SqlBuilder;
