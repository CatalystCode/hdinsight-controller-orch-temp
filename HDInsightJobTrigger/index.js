module.exports = function (context, input) {
    context.log('Node.js queue trigger function processed work item', input);
    
    return context.done();
}