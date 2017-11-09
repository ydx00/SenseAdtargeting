namespace go thrift_service

service AdTargetingService {
    string search(1:string appId,2:string userId,3:string broadcasterId,4:string requestId);

    string searchV2(1:string appId,2:string userId,3:string broadcasterId,4:i32 adMode,5:string requestId);

    string preload(1:string appId,2:string userId,3:string requestId);

    string preloadV2(1:string appId,2:string userId,3:i32 adMode,4:string requestId);

    i64 userCoverage(1:string appId,2:string conditions);
}