(function dsbuilder(attr){
    var urlBuilder = "jdbc:documentdb://" + attr["server"] + ":" + attr["port"] + "/" + attr["v-database"];
    return [urlBuilder];
})

