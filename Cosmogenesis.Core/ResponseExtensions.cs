using System.Net;
using Microsoft.Azure.Cosmos;

namespace Cosmogenesis.Core;

static class ResponseExtensions
{
    public static DbChange<T> DbChangeFromErrorStatus<T>(this ResponseMessage responseMessage) where T : DbDoc => DbChangeFromErrorStatus<T>(responseMessage.StatusCode);
    public static DbChange<T> DbChangeFromErrorStatus<T>(this HttpStatusCode statusCode) where T : DbDoc => statusCode switch
    {
        HttpStatusCode.PreconditionFailed => DbChange<T>.ETagChanged,
        HttpStatusCode.Conflict => DbChange<T>.AlreadyExists,
        HttpStatusCode.NotFound => DbChange<T>.Missing,
        _ => throw ExceptionFromErrorStatus(statusCode)
    };

    public static CreateResult<T> CreateResultFromErrorStatus<T>(this ResponseMessage responseMessage) where T : DbDoc => CreateResultFromErrorStatus<T>(responseMessage.StatusCode);
    public static CreateResult<T> CreateResultFromErrorStatus<T>(this HttpStatusCode statusCode) where T : DbDoc => statusCode switch
    {
        HttpStatusCode.Conflict => CreateResult<T>.AlreadyExists,
        _ => throw ExceptionFromErrorStatus(statusCode)
    };
    public static DbConflictType CreateConflictTypeFromErrorStatus(this ResponseMessage responseMessage) => responseMessage.StatusCode switch
    {
        HttpStatusCode.Conflict => DbConflictType.AlreadyExists,
        _ => throw ExceptionFromErrorStatus(responseMessage.StatusCode)
    };

    public static ReplaceResult<T> ReplaceResultFromErrorStatus<T>(this ResponseMessage responseMessage) where T : DbDoc => ReplaceResultFromErrorStatus<T>(responseMessage.StatusCode);
    public static ReplaceResult<T> ReplaceResultFromErrorStatus<T>(this HttpStatusCode statusCode) where T : DbDoc => statusCode switch
    {
        HttpStatusCode.PreconditionFailed => ReplaceResult<T>.ETagChanged,
        HttpStatusCode.NotFound => ReplaceResult<T>.Missing,
        _ => throw ExceptionFromErrorStatus(statusCode)
    };
    public static DbConflictType ReplaceConflictTypeFromErrorStatus(this ResponseMessage responseMessage) => responseMessage.StatusCode switch
    {
        HttpStatusCode.PreconditionFailed => DbConflictType.ETagChanged,
        HttpStatusCode.NotFound => DbConflictType.Missing,
        _ => throw ExceptionFromErrorStatus(responseMessage.StatusCode)
    };

    public static DbConflictType DeleteConflictTypeFromErrorStatus(this ResponseMessage responseMessage) => DeleteConflictTypeFromErrorStatus(responseMessage.StatusCode);
    public static DbConflictType DeleteConflictTypeFromErrorStatus(this HttpStatusCode statusCode) => statusCode switch
    {
        HttpStatusCode.PreconditionFailed => DbConflictType.ETagChanged,
        HttpStatusCode.NotFound => DbConflictType.Missing,
        _ => throw ExceptionFromErrorStatus(statusCode)
    };

    public static BatchResult BatchResultFromErrorStatus(this TransactionalBatchResponse responseMessage) => BatchResultFromErrorStatus(responseMessage.StatusCode);
    public static BatchResult BatchResultFromErrorStatus(this HttpStatusCode statusCode) => statusCode switch
    {
        HttpStatusCode.PreconditionFailed => BatchResult.ETagChanged,
        HttpStatusCode.Conflict => BatchResult.AlreadyExists,
        HttpStatusCode.NotFound => BatchResult.Missing,
        _ => throw ExceptionFromErrorStatus(statusCode)
    };

    public static Exception ExceptionFromErrorStatus(this ResponseMessage responseMessage) => ExceptionFromErrorStatus(responseMessage.StatusCode);
    public static Exception ExceptionFromErrorStatus(this HttpStatusCode statusCode) => statusCode switch
    {
        HttpStatusCode.TooManyRequests => new DbOverloadedException(),
        HttpStatusCode.RequestEntityTooLarge => new DbRequestTooLargeException(),
        _ => new DbUnknownStatusCodeException(statusCode)
    };

}
