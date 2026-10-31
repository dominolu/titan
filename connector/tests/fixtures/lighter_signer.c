/* Test-only ABI fixture. This is not cryptography and cannot sign a real transaction. */
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
typedef struct { char* str; char* err; } StrOrErr;
typedef struct { uint8_t txType; char* txInfo; char* txHash; char* messageToSign; char* err; } SignedTxResponse;
char* CreateClient(char* url,char* key,int chain,int api,int64_t account){return chain==304 && api==2 && account>0 && strlen(key)>0 ? NULL : strdup("bad fixture config");}
StrOrErr CreateAuthToken(int64_t deadline,int api,int64_t account){StrOrErr r={strdup("fixture-auth"),NULL};return r;}
void Free(void* p){free(p);}
SignedTxResponse SignCreateOrder(int64_t market,int64_t id,int64_t amount,int price,int ask,int type,int tif,int reduce,int trigger,int64_t expiry,int64_t integrator,int takerfee,int makerfee,uint8_t behavior,uint8_t equality,uint8_t skip,int64_t nonce,int api,int64_t account){
 SignedTxResponse r={0};if(market!=0||id!=1||amount!=20||price!=1001||ask!=0||type!=0||tif!=2||reduce!=1||trigger!=0||expiry!=-1||integrator!=0||takerfee!=0||makerfee!=0||behavior!=0||equality!=0||skip!=0||api!=2||nonce<5){r.err=strdup("incorrect native ABI arguments");return r;}
 char b[128];snprintf(b,sizeof(b),"{\"AccountIndex\":%lld,\"ClientOrderIndex\":%lld,\"Nonce\":%lld}",(long long)account,(long long)id,(long long)nonce);r.txType=14;r.txInfo=strdup(b);r.txHash=strdup("fixture-hash");return r;
}
SignedTxResponse SignCancelOrder(int64_t market,int64_t id,uint8_t skip,int64_t nonce,int api,int64_t account){SignedTxResponse r={15,strdup("{}"),strdup("fixture-cancel"),NULL,NULL};return r;}
SignedTxResponse SignModifyOrder(int64_t market,int64_t id,int64_t amount,int64_t price,int64_t trigger,int64_t integrator,int takerfee,int makerfee,uint8_t behavior,uint8_t equality,uint8_t skip,int64_t nonce,int64_t version,int api,int64_t account){SignedTxResponse r={17,strdup("{}"),strdup("fixture-modify"),NULL,NULL};return r;}
SignedTxResponse SignBinaryOptionsOperatorSettleMarket(void* req,uint8_t skip,int64_t nonce,int api,int64_t account){SignedTxResponse r={0,NULL,NULL,NULL,strdup("fixture ABI marker only")};return r;}
