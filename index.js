function memoize_one(func) {
  let memo = {
    args:[],
    result:undefined
  };
  return function(...args) {
    if (memo.args.length===args.length) {
      let isSame = true;
      for (let i=0; i<args.length; i++) {
        if (args[i]===memo.args[i]) {
          continue;
        }
        else {
          isSame=false;
          break;
        }
      }
      return memo.result;
    }
    memo.result = func(...args);
    memo.args = args;
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
  }
  
  buildSQL(query) {
    if (!this._isValidQuery(query)) {
      throw new SyntaxError("Query not valid");
    }
    let ret = {};
    let querySelect = query["SELECT"];
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
      let ref = config["REFERENCE"] || {};
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
      let definedAttrs = schema[e]["ATTRIBUTE"];
      let queryAttrs = Object.keys(querySelect[e]);
      for (let a of queryAttrs) {
        if ( !(a in definedAttrs) ){
          return false;
        }
      }
    }
    // Entities in query.FILTER is defined in schema
    for (let f of query["FILTER"]) {
      for (let e of Object.keys(f)) {
        if (! (e in schema)) {
          return false;
        }
      }
    }
    // Entities attributes in query.FILTER is defined in schema
    for (let f of query["FILTER"]) {
      for (let e of Object.keys(f)){
        let definedAttrs = schema[e]["ATTRIBUTE"];
        let queryAttrs = Object.keys(f[e]);
        for (let a of queryAttrs) {
          if ( !(a in definedAttrs) ){
            return false;
          }
        }
      }
    }
    // Entities attributes filter operator is valid
    this._isValidFilter(query["FILTER"]);
  }

  _isValidFilter(filterNode) {
    let {schema} = this;
    let {op,args} = filterNode;
    switch (op) {
      // Monadic
      case "NOT":
        if (args.length !== 1) {
          return false;
        }
        if (arg[0].op === "NOT") {
          return false;
        }
     
      // Logical+Polyadic
      case "AND":
      case "OR":
        if (args.length <=1) {
          return false;
        }
     
      // Comparison+Dyadic
      case "=":
      case "<":
      case ">":
      case "<=":
      case ">=":
      case "!<":
      case "!=":
      case "<>":
      case "!>":
      case "!<=":
      case "!>=":
      case "LIKE":
      case "NOT LIKE":
      case "IS":
      case "IS NOT":
        if (args.length !== 2) {
          return false;
        }
        if ( !(args.every(_isValidFilterLeaf)) ) {
          return false;
        }
       
      // Comparison+Triadic
      case "BETWEEN":
      CASE "NOT BETWEEN":
        if (args.length !== 3) {
          return false;
        }
        if ( !(args.every(_isValidFilterLeaf)) ) {
          return false;
        }
     
      // Comparison+Polyadic
      case "IN":
      case "NOT IN":
        if (args.length < 2) {
          return false;
        }
        if ( !(args.every(_isValidFilterLeaf)) ) {
          return false;
        }
     
      default:
        return false;
    }
    for (let arg of args) {
      if ( !_isValidFilter(arg) ) {
        return false;
      }
    }
    return true;
  }

  _isValidFilterLeaf(leaf) {
    let {schema} = this;
    if (leaf === null
        || typeof leaf === "string"
        || typeof leaf === "number"
        || _isValidEntityAttribute(leaf)
        ) {
      return true;
    }
    return false;
  }
  
  _isValidEntityAttribute(leaf) {
    let {schema} = this;
    let filterableAttributes = this._getFilterableAttribute(schema);
    if (leaf !== null
        && typeof leaf === "object"
        && typeof leaf.attribute === "string"
        && leaf.attribute) {
      return true;
    }
    return false;
  }
  


  _generateSQL(entity,query) {
    let {schema} = this;
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
    let querySelect = query["SELECT"];
    let queryFilter = query["FILTER"];
    let statements = [];
    let includeCTE = {};
    includeCTE[entity] = null;
    for (let filterSet of queryFilter) {
      for (let refE of Object.keys(filterSet)) {
        if (refE in includeCTE) {
          continue;
        }
        else {
          for (let interE of this._resolveJoinOrder(entity,refE)) {
            includeCTE[interE] = null;
          }
        }
      }
    }
    for ( let e of Object.keys(includeCTE) ) {
      statements.push(schema[e]["CTE"]);
    }
    return "WITH "+statements.join(',');
  }

  _generateSelectStatement(entity,query) {
    let {schema} = this;
    let querySelect = query["SELECT"];
    let queryAttrs = {...querySelect[entity]};
    queryAttrs["__ID__"] = null;
    if ("REFERENCE" in schema[entity]) {
      for ( let [refEntity,id] of Object.entries(schema[entity]["REFERENCE"]) ) {
        if (refEntity in querySelect) {
          queryAttrs[`__REF__${refEntity}`];
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
    let {schema} = this;
    let queryFilter = query["FILTER"];
    let entityTable = schema[entity]["TABLE"];
    let entityID = schema[entity]["ID"];
    let orFilters = [];
    let binds = [];
    for (let filterSet of queryFilter) {
      let andFilters = [];
      for (let [refEntity, entityFilter] of Object.entries(filterSet) ) {
        let subJoinStatement = this._generateJoinStatement(entity,refEntity);
        let subFilterStatement = [];
        for (let [attr,attrFilter] of Object.entries(entityFilter) ){
          let [opStatement,opBinds] = this._generateOperatorStatement(attrFilter,binds.length+1);
          subFilterStatement.push(`"${refEntity}"."${attr}" ${opStatement}`);
          binds.push(...opBinds);
        }
        subFilterStatement = subFilterStatement.join(" AND ");
        let filterString = `"${entity}"."__ID__" IN (SELECT "${entity}"."__ID__" FROM ${subJoinStatement} WHERE ${subFilterStatement})`;
        andFilters.push(filterString);
      }
      orFilters.push(andFilters.join(" AND "));
    }
    // Return
    if (orFilters.length===0) {
      return ["",[]];
    }
    else {
      let statement = orFilters.join(" OR ");
      return [`WHERE ${statement}`,binds];
    }
  }

  _generateJoinStatement(entity1,entity2) {
    // Assuming entities joinable
    let {schema} = this;
    let joinStatement = [entity1];
    let joinOrder = this._resolveJoinOrder(entity1,entity2);
    for (let i=0,j=1; j<joinOrder.length; i++,j++) {
      let e1 = joinOrder[i];
      let e2 = joinOrder[j];
      let s = `JOIN "${e2}" ON `+this._generateAdjacentJoinOnStatement(e1,e2);
      joinStatement.push(s);
    }
    return joinStatement.join(' ');
  }

  _resolveJoinOrder(entity1,entity2){
    // Assuming both entities are found in schema and joinable
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
    let joinStatement = null;
    // Either entity1 references entity2
    if ("REFERENCE" in schema[entity1] &&
        entity2 in schema[entity1]["REFERENCE"]) {
      joinStatement = `"${entity1}"."__REF__${entity2}"="${entity2}"."__ID__"`;
    }
    // Or entity2 references entity1
    else if ("REFERENCE" in schema[entity2] &&
        entity1 in schema[entity2]["REFERENCE"]) {
      joinStatement = `"${entity1}"."__ID__"="${entity2}"."__REF__${entity1}"`;
    }
    else {
      throw new Error("Not adjacent entities");
    }
    return joinStatement;
  }

  _generateOperatorStatement(filterTree,count=0) {
    let {op,args} = filterTree;
    let statement = null;
    let binds = null;
   
    if (op === "NOT"){
      let node = args[0];
      let [statement,binds] = _generateOperatorStatement(node,count+binds.length);
      statement = `NOT ${statement}`;
      return [statement,binds];
    }
   
    else if (
        op === "AND"
        || op === "OR"
        ) {
      statement = [];
      binds = [];
      for (let node of args) {
        let [tmpStatement,tmpBinds] = _generateOperatorStatement(node,count+binds.length);
        statement.push( `(${tmpStatement})` );
        binds.push(...tmpBinds);
      }
      statement = statement.join(` ${op} `);
      return [statement,binds];
    }
     
    else if (
        op === "="
        || op === "<"
        || op === ">"
        || op === "<="
        || op === ">="
        || op === "!<"
        || op === "!>"
        || op === "!="
        || op === "LIKE"
        || op === "NOT LIKE"
        || op === "IS"
        || op === "IS NOT"
        ) {
      statement = [];
      binds = [];
      for (let arg of args) {
        if ( _isValidEntityAttribute(arg) ) {
          statement.push(arg.attribute);
        }
        else {
          statement.push( `:${count+binds.length}` );
          binds.push(arg);
        }
      }
      statement = statement.join(` ${op} `);
      return [statement,binds];
    }
   
    else if (
        op === "BETWEEN"
        || op === "NOT BETWEEN"
        ) {
      statement = [];
      binds = [];
      for (let arg of args) {
        if ( _isValidEntityAttribute(arg) ) {
          statement.push(arg.attribute);
        }
        else {
          statement.push( `:${count+binds.length}` );
          binds.push(arg);
        }
      }
      statement = `${statement[0]} ${op} ${statement[1]} AND ${statement[2]}`;
      return [statement,binds];
    }
   
    else if (
        op === "IN"
        || op === "NOT IN"
        ) {
      statement = [];
      binds = [];
      for (let arg of args) {
        if ( _isValidEntityAttribute(arg) ) {
          statement.push(arg.attribute);
        }
        else {
          statement.push( `:${count+binds.length}` );
          binds.push(arg);
        }
      }
      statement = `${statement[0]} %{op} (${statement.slice(1).join(',')})`;
      return [statement,binds];
    }
   
    else {
      throw new InternalError("Filter parsing error");
    }
  }
}

SqlBuilder.VALID_FILTER_OPERATOR = {
  "<":null,
  "=":null,
  ">":null,
  "<=":null,
  ">=":null,
  "!<":null,
  "!=":null,
  "<>":null,
  "!>":null,
  "!<=":null,
  "!>=":null,
  "LIKE":null,
  "NOT LIKE":null,
  "IS":null,
  "IS NOT":null,
  "BETWEEN":null,
  "NOT BETWEEN":null,
  "IN":null,
  "NOT IN":null,
};

module.exports = SqlBuilder;
