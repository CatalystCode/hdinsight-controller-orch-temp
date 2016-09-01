module.exports = function (context, checkTimer) {
    context.log('Node.js queue trigger function processed work item', checkTimer);
    
    return context.done();
};