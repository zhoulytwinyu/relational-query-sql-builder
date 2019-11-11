function memoize_one(func) {
  let memo = {
    args:undefined,
    result:undefined
  };
  return function(...args) {
    if (memo.args===undefined) {
      memo.result = func(args);
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
      memo.result = func(args);
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
  }
  
  buildSQL(query) {
    if (!this._isValidQuery(query)) {
      throw new SyntaxError("Query not valid");
    }
    let ret = {};
    let querySelect = query["select"];
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
    if ( !this._isValidQuerySelect(query.select) ){
      return false;
    }
    query.filter = query.filter || null;
    if ( "filter" in query
        && !this._isValidQueryFilter(query.filter)
        ) {
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
  }

  _isValidQueryFilter(queryFilter) {
    if (queryilter === null) {
      return true;
    }
    let {schema} = this;
    let dfsStack = [queryFilter];
    let parentOpTypeStack = [null];
    while (dfsStack.length!==0 ) {
      let fNode = dfsStack.pop();
      let parentOp = parentOpTypeStack.pop();
      let valid = this._isValidFilterNode(fNode,parentOp);
      if (!valid) {
        return false;
      }
      if (let fn in fNode.filters) {
        dfsStack.push(fn);
        parentOpTypeStack.push(fNode.op);
      }
    }
    return true;
  }
  
  _isValidFilterNode(filterNode,parentOp) {
    /* 
     * filterNode comes in 2 flavours:
     * 1) NOT, AND, OR
     *    They can nest other filters; they do not accept variables
     *    e.g. AND(filter1,filter2,OR(filter3,filter4))
     * 2) =, <, >, ...
     *    They canot nest other filters; thye accept variables
     *    >(var,var2)
     */
    let {op} = filterNode;
    // check node transition
    switch(parentOp) {
      // These can be followed by any nested filter
      case null:
      case "NOT":
      case "AND":
      case "OR":
        break;
      // These cannot be followed by an nested filter, instead, must be followed by filter variables.
      case "=":
      case "<":
      case ">":
      case "<=":
      case ">=":
      case "!=":
      case "LIKE":
      case "NOT LIKE":
      case "IS":
      case "IS NOT":
      case "BETWEEN":
      case "NOT BETWEEN":
      case "IN":
      case "NOT IN":
      default:
        return false;
    }
    // Check nested filter counts
    switch (op) {
      // Monadic
      case "NOT":
        if (!("filters" in filterNode)
            || filterNode.filters.length !== 1
            ) {
          return false;
        }
        break;
      // Logical+Polyadic
      case "AND":
      case "OR":
        if (!("filters" in filterNode) 
            || filterNode.filters..length <=1
            ) {
          return false;
        }
        break;
      case "=":
      case "<":
      case ">":
      case "<=":
      case ">=":
      case "!=":
      case "LIKE":
      case "NOT LIKE":
      case "IS":
      case "IS NOT":
      case "BETWEEN":
      case "NOT BETWEEN":
        if ( "filters" in filterNode ) {
          return false;
        }
        break;
      default:
        return false;
    }
    // Check variable counts
    switch (op) {
      case "NOT":
      case "AND":
      case "OR":
        if ("variables" in filterNode) {
          return false;
        }
        break;
      case "=":
      case "<":
      case ">":
      case "<=":
      case ">=":
      case "!=":
      case "LIKE":
      case "NOT LIKE":
      case "IS":
      case "IS NOT":
        if (!("variables" in filterNode)
            || filterNode.variables.length !== 2
            ) {
          return false;
        }
        break;
      case "BETWEEN":
      case "NOT BETWEEN":
        if (!("variables" in filterNode)
            || filterNode.variables.length < 3
            ) {
          return false;
        }
        break;
      default:
        return false;
     
    }
    // Check entity/attributes defined in filter variables against schema
    if ("variables" in filterNode) {
      for (let v of filterNode.variables){
        if ( ! this._isValidFilterVariable(v) ) {
          return false;
        }
      }
    }
    return true;
  }

  _isValidFilterVariable(filterVariable) {
    let {schema} = this;
    if (filterVariable === null
        || typeof filterVariable === "string"
        || typeof filterVariable === "number"
        || _isValidEntityAttribute(filterVariable)
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
        && (filterVariable.attribute in this.schema[filterVariable.entity])
        ) {
      return true;
    }
    return false;
  }

  _getFilterEntities(queryFilter) {
    if (queryFilter === null) {
      return [];
    }
    let dfsStask = [queryFilter];
    let ret = [];
    while (dfsStack.length !== 0){
      let fNode = dfsStack.pop();
      if ("filters" in fNode) {
        dfsStack.push(...fNode.filters);
      }
      if ("variables" in fNode){
        for (let v of fNode.variables){
          if (typeof fNode === "object"
              && fNode !== null
              ){
            ret.push(fNode.entity);
          }
        }
      }
    }
    return ret;
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
    let entities = this._getFilterEntities(queryFilter);
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
    if (!("filter" in query)
        || query["filter"] === null
        ) {
      return "";
    }
    let {schema} = this;
    let queryFilter = query["filter"];
    let entities = this._getFilterEntities(queryFilter);
    let joinStatement = this._generateJoinStatement(entity,entities);
    let filterStatement = this._generateFilterStatement(queryFilter);
    return `WHERE "${entity}"."__ID__" IN (SELECT "${entity}"."__ID__" FROM ${joinStatement} WHERE ${filterStatement})`;
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
    let joinStatement = null;
    // Either entity1 references entity2
    if ("REFERENCE" in schema[entity1] &&
        entity2 in schema[entity1]["REFERENCE"]) {
      joinOnStatement = `"${entity1}"."__REF__${entity2}"="${entity2}"."__ID__"`;
    }
    // Or entity2 references entity1
    else if ("REFERENCE" in schema[entity2] &&
        entity1 in schema[entity2]["REFERENCE"]) {
      joinOnStatement = `"${entity1}"."__ID__"="${entity2}"."__REF__${entity1}"`;
    }
    else {
      throw new InternalError("Not adjacent entities");
    }
    return joinOnStatement;
  }

  _generateFilterStatement(filterNode,bindsCount) {
    let {op,filters,variables} = filterNode;
    if (op === "NOT"){
      let fn = filters[0];
      let [statement,binds] = this._generateFilterStatement(fn,bindsCount+binds.length);
      statement = `NOT ${statement}`;
      return [statement,binds];
    }
    else if (
        op === "AND"
        || op === "OR"
        ) {
      let statement = [];
      let binds = [];
      for (let fn of filters) {
        let [tmpStatement,tmpBinds] = _generateOperatorStatement(fn,count+binds.length);
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
        || op === "!="
        || op === "LIKE"
        || op === "NOT LIKE"
        || op === "IS"
        || op === "IS NOT"
        ) {
      let statement = [];
      let binds = [];
      for (let fv of variables) {
        if ( this._isValidEntityAttribute(fv) ) {
          statement.push(`${fv.entity}.${fv.attribute}`);
        }
        else {
          statement.push( `:${count+binds.length}` );
          binds.push(fv);
        }
      }
      statement = statement.join(` ${op} `);
      return [statement,binds];
    }
   
    else if (
        op === "BETWEEN"
        || op === "NOT BETWEEN"
        ) {
      let statement = [];
      let binds = [];
      for (let fv of variables) {
        if ( this._isValidEntityAttribute(fv) ) {
          statement.push(`${fv.entity}.${fv.attribute}`);
        }
        else {
          statement.push( `:${count+binds.length}` );
          binds.push(fv);
        }
      }
      statement = `${statement[0]} ${op} ${statement[1]} AND ${statement[2]}`;
      return [statement,binds];
    }
   
    else if (
        op === "IN"
        || op === "NOT IN"
        ) {
      let statement = [];
      let binds = [];
      for (let fv of variables) {
        if ( this._isValidEntityAttribute(fv) ) {
          statement.push(`${fv.entity}.${fv.attribute}`);
        }
        else {
          statement.push( `:${count+binds.length}` );
          binds.push(fv);
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

module.exports = SqlBuilder;
