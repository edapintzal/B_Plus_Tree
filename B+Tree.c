

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define ORDER 4
#define KEY_SIZE 101
#define MAX_RUNS 100
#define MAX_LINE 256
#define MAX_RECORDS 10000
#define HEAP_SIZE 512
#define MAX_KEY_LEN 101
int splitCountSeq=0;
int splitCountBulk=0;
int treeHeightSeq=0;
int treeHeightBulk=0;
int numberOfNodesSeq=0;
int numberOfNodesBulk=0;
double avgSeekTimeSeq=0.0;
double avgSeekTimeBulk=0.0;




typedef struct Record {
    char id[32];
    char university[101];
    char department[101];
    float points;
    int frozen;
} Record;


typedef struct FileBuffer {
    FILE* file;
    Record current;
    int hasData;
} FileBuffer;



Record allRecords[7007];
char *records[7007];




typedef struct UniNode {
    char name[101];
    float points;
    struct UniNode* next;
} UniNode;

typedef struct Node {
    int isLeaf;
    int numKeys;
    char keys[ORDER - 1][KEY_SIZE];
    struct Node* children[ORDER];
    struct Node* next;
    struct Node* prev;
    UniNode* uniHeads[ORDER - 1];
} Node;

Node* root = NULL;
void delimeterCSV(char* line, char tokens[][101]);
enum { false, true };
Node* createNode(int isLeaf);
int contains(Node* node, const char* key);
Node* findLeaf(Node* node, const char* key);
void splitChild(Node* parent, int idx);
void insertNonFull(Node* node, const char* key);
void insertKey(const char* key);
void bulkLoad(int n, char* records[]);
int readSortedCsv(const char* filename);
void printTree(Node* node, int level);
void findUniNode(Node* node, const char* key, int rank);




int compareRecords(const Record* a, const Record* b) {
    int cmp = strcmp(a->department, b->department);

    if (cmp == 0) {
        // puan büyük olan önce
        return (b->points > a->points) - (b->points < a->points);
    }

    return cmp;
}

//Tırnakları kaldırmak için
void stripQuotes(char* dest, const char* src) {
    int j = 0;

    for (int i = 0; src[i]; i++) {
        if (src[i] != '"') dest[j++] = src[i];
    }

    dest[j] = '\0';
}



// Hepsi bir arada parse: id, univ, department, points
int parseLine(char* line, Record* rec) {
    char tok[4][KEY_SIZE];
    delimeterCSV(line, tok);



    strncpy(rec->id, tok[0], sizeof(rec->id)-1);
    rec->id[sizeof(rec->id)-1] = '\0';

    strncpy(rec->university, tok[1], sizeof(rec->university)-1);
    rec->university[sizeof(rec->university)-1] = '\0';

    // departmanı tırnaksız kopyala
    stripQuotes(rec->department, tok[2]);

    // puanı dönüştür
    char* endptr;
    rec->points = strtof(tok[3], &endptr);
    if (endptr == tok[3]) rec->points = 0.0f;

    rec->frozen = 0;
    return 1;
}

//––– 5) Bir run dosyası yaz (department zaten temiz)
void writeRun(Record* buf, int sz, int runIndex) {
    char fname[64];
    snprintf(fname, sizeof(fname), "run_%d.csv", runIndex);
    FILE* f = fopen(fname, "w");
    if (!f) return;
    for (int i = 0; i < sz; i++) {

        // department zaten stripQuotes ile temizlendiği için buraya direkt koyuyoruz
        fprintf(f, "%s,%s,\"%s\",%.2f\n",
                buf[i].id,
                buf[i].university,
                buf[i].department,
                buf[i].points);
    }
    fclose(f);
}

//––– 6) Replacement‐selection: önce heap’ı doldur, sonra run’lar üret
void replacementSelectionSort(const char* inFile, int* totalRuns) {
    FILE* fp = fopen(inFile, "r");
    if (!fp) exit(1);

    char line[MAX_LINE];
    fgets(line, sizeof(line), fp);  // başlığı atla

    Record heap[HEAP_SIZE];
    int heapSize = 0;
    Record outBuf[HEAP_SIZE * 10];
    int out = 0;
    int runIdx = 0;

    // ilk yükleme
    while (heapSize < HEAP_SIZE && fgets(line, sizeof(line), fp)) {
        if (parseLine(line, &heap[heapSize])) heapSize++;
    }

    // run üretme döngüsü
    while (heapSize > 0) {
        int minI = -1;
        for (int i = 0; i < heapSize; i++) {
            if (!heap[i].frozen &&
                (minI < 0 || compareRecords(&heap[i], &heap[minI]) < 0)) {
                minI = i;
            }
        }
        // tüm elemanlar frozen -> yeni run
        if (minI < 0) {
            writeRun(outBuf, out, runIdx++);
            out = 0;
            for (int i = 0; i < heapSize; i++)
                heap[i].frozen = 0;
            continue;
        }

        // en küçük elemanı al
        outBuf[out++] = heap[minI];

        // yeni satır oku
        if (fgets(line, sizeof(line), fp)) {
            Record nr;
            if (parseLine(line, &nr)) {
                nr.frozen = (compareRecords(&nr, &heap[minI]) < 0);
                heap[minI] = nr;
            }
        } else {
            // heap’ten kaldır
            for (int k = minI; k < heapSize - 1; k++)
                heap[k] = heap[k+1];
            heapSize--;
        }
    }

    // kalan run
    if (out > 0) writeRun(outBuf, out, runIdx++);
    fclose(fp);
    *totalRuns = runIdx;
}

//––– 7) Multi‐way merge: tüm run_*.csv’leri birleştir
void multiWayMerge(int totalRuns, const char* outFile) {
    FileBuffer buf[MAX_RUNS];
    FILE* out = fopen(outFile, "w");
    if (!out) return;

    // her run dosyası için ilk satırı oku
    for (int i = 0; i < totalRuns; i++) {
        char fname[64];
        snprintf(fname, sizeof(fname), "run_%d.csv", i);
        buf[i].file = fopen(fname, "r");
        buf[i].hasData = 0;
        if (buf[i].file) {
            char line[MAX_LINE];
            if (fgets(line, sizeof(line), buf[i].file) &&
                parseLine(line, &buf[i].current)) {
                buf[i].hasData = 1;
            }
        }
    }

    // merge döngüsü
    while (1) {
        int minI = -1;
        for (int i = 0; i < totalRuns; i++) {
            if (buf[i].hasData &&
                (minI < 0 ||
                 compareRecords(&buf[i].current, &buf[minI].current) < 0)) {
                minI = i;
            }
        }
        if (minI < 0) break;

        // en küçük kaydı yaz, department tırnaklı
        Record* r = &buf[minI].current;
        fprintf(out, "%s,%s,\"%s\",%.2f\n",
                r->id,
                r->university,
                r->department,
                r->points);

        // o run’dan ilerle
        char line[MAX_LINE];
        if (fgets(line, sizeof(line), buf[minI].file) &&
            parseLine(line, &buf[minI].current)) {
            buf[minI].hasData = 1;
        } else {
            buf[minI].hasData = 0;
            fclose(buf[minI].file);
        }
    }

    fclose(out);
}









Node* createNode(int isLeaf) {
    Node* node = (Node*)malloc(sizeof(Node));
    node->isLeaf = isLeaf;
    node->numKeys = 0;
    node->next = NULL;
    node->prev = NULL;
    for (int i = 0; i < ORDER; i++) node->children[i] = NULL;
    for (int i = 0; i < ORDER - 1; i++) node->uniHeads[i] = NULL;
    numberOfNodesSeq++;
    return node;
}




void bulkLoad(int n, char *records[]) {
    //all records arrayin icini dolasirken kullanıcaz
    int allIndex = 0;

    // unique anahtarları tutuyor
    char (*uniqueKeys)[KEY_SIZE] = malloc(n * KEY_SIZE);
    int uniqueCount = 0;
    for (int i = 0; i < n; i++) {
        if (i == 0 || strcmp(records[i], records[i - 1]) != 0) {
            strncpy(uniqueKeys[uniqueCount], records[i], KEY_SIZE-1);
            uniqueKeys[uniqueCount][KEY_SIZE-1] = '\0';
            uniqueCount++;
        }
    }

    // yaprak basına kac anahtar
    int keysPerLeaf = ORDER - 1;
    int numLeaves   = (uniqueCount + keysPerLeaf - 1) / keysPerLeaf;

    // leaf nodeları double linkleyecez
    Node **leaves  = malloc(numLeaves * sizeof(Node*));
    Node  *prev    = NULL;
    int    uIdx    = 0;  // uniqueKeys indeksi

    for (int L = 0; L < numLeaves; L++) {
        numberOfNodesBulk++;
        Node *leaf = createNode(1);
        leaf->numKeys = 0;
        leaf->prev    = prev;
        if (prev) prev->next = leaf;

        // Bu leaf için up to keysPerLeaf anahtar koy
        for (int j = 0; j < keysPerLeaf && uIdx < uniqueCount; j++, uIdx++) {
            // anahtarları doldur
            strncpy(leaf->keys[j], uniqueKeys[uIdx], KEY_SIZE-1);
            leaf->keys[j][KEY_SIZE-1] = '\0';
            leaf->uniHeads[j] = NULL;
            leaf->numKeys++;

            // aynı departmandaki ünileri ekleme
            while (allIndex < n &&
                   strcmp(allRecords[allIndex].department, uniqueKeys[uIdx]) == 0) {
                UniNode *node = malloc(sizeof(UniNode));
                strncpy(node->name,   allRecords[allIndex].university, sizeof node->name);
                node->points = allRecords[allIndex].points;
                node->next   = NULL;

                if (!leaf->uniHeads[j]) {
                    leaf->uniHeads[j] = node;
                } else {
                    UniNode *cur = leaf->uniHeads[j];
                    while (cur->next) cur = cur->next;
                    cur->next = node;
                }
                allIndex++;
            }
        }

        leaf->next = NULL;
        leaves[L]  = leaf;
        prev       = leaf;
    }
    free(uniqueKeys);

    // bottom-up cıkıyoruz
    Node **level = leaves;
    int    cnt   = numLeaves;
    while (cnt > 1) {
        int nidx    = (cnt + ORDER - 1) / ORDER;
        Node **next = malloc(nidx * sizeof(Node*));
        int ni = 0;
        for (int i = 0; i < cnt; i += ORDER) {
            Node *ix = createNode(0);
            ix->numKeys = 0;

            for (int j = 0; j < ORDER && i + j < cnt; j++) {
                ix->children[j] = level[i+j];
                if (j > 0) {
                    strncpy(ix->keys[j-1],
                            level[i+j]->keys[0],
                            KEY_SIZE-1);
                    ix->keys[j-1][KEY_SIZE-1] = '\0';
                    ix->numKeys++;
                }
            }
            next[ni++] = ix;
        }
        free(level);
        level = next;
        cnt = ni;
        treeHeightBulk++;
    }

    // 5) Kökü ayarla
    root = level[0];
    free(level);
}


double avgBulkSeekTime(){

    double totalTime = 0.0;
    for (int i = 0; i < 358; i++) {
        clock_t start = clock();
        Node* leaf = findLeaf(root, records[i]);
        clock_t end = clock();
        totalTime += (double)(end - start) / CLOCKS_PER_SEC;
    }
    return totalTime / 358;
}

//node var mı yok mu kontrolü
int contains(Node* node, const char* key) {
    while (node && !node->isLeaf) {
        int i = 0;
        while (i < node->numKeys && strcmp(key, node->keys[i]) >= 0) i++;
        node = node->children[i];
    }
    for (int i = 0; node && i < node->numKeys; i++) {
        if (strcmp(key, node->keys[i]) == 0) return 1;
    }
    return 0;
}
//leaf node arama
Node* findLeaf(Node* node, const char* key) {
    while (node && !node->isLeaf) {
        int i = 0;
        while (i < node->numKeys && strcmp(key, node->keys[i]) >= 0) i++;
        node = node->children[i];
    }
    return node;
}
//split işlemi
void splitChild(Node* parent, int idx) {
    Node* child = parent->children[idx];
    Node* newChild = createNode(child->isLeaf);
    int mid = child->numKeys / 2;

    if (child->isLeaf) {
        newChild->numKeys = child->numKeys - mid;
        for (int i = 0, j = mid; j < child->numKeys; i++, j++) {
            strncpy(newChild->keys[i], child->keys[j], KEY_SIZE - 1);
            newChild->keys[i][KEY_SIZE - 1] = '\0';
            newChild->uniHeads[i] = child->uniHeads[j];
            child->uniHeads[j] = NULL;
        }
        child->numKeys = mid;
        newChild->next = child->next;
        child->next = newChild;
    } else {
        newChild->numKeys = child->numKeys - mid - 1;
        for (int i = 0, j = mid + 1; j < child->numKeys; i++, j++)
            strncpy(newChild->keys[i], child->keys[j], KEY_SIZE);
        for (int i = 0, j = mid + 1; j <= child->numKeys; i++, j++)
            newChild->children[i] = child->children[j];
        child->numKeys = mid;
    }

    for (int i = parent->numKeys; i > idx; i--)
        parent->children[i + 1] = parent->children[i];
    parent->children[idx + 1] = newChild;

    for (int i = parent->numKeys; i > idx; i--)
        strncpy(parent->keys[i], parent->keys[i - 1], KEY_SIZE);

    strncpy(parent->keys[idx],
            child->isLeaf ? newChild->keys[0] : child->keys[mid],
            KEY_SIZE);

    parent->numKeys++;
    splitCountSeq++;
}

void insertNonFull(Node* node, const char* key) {
    int i = node->numKeys - 1;
    if (node->isLeaf) {
        while (i >= 0 && strcmp(key, node->keys[i]) < 0) {
            strncpy(node->keys[i + 1], node->keys[i], KEY_SIZE);
            node->uniHeads[i + 1] = node->uniHeads[i];
            i--;
        }
        strncpy(node->keys[i + 1], key, KEY_SIZE - 1);
        node->keys[i + 1][KEY_SIZE - 1] = '\0';
        node->uniHeads[i + 1] = NULL;
        node->numKeys++;
    } else {
        while (i >= 0 && strcmp(key, node->keys[i]) < 0) i--;
        i++;
        if (node->children[i]->numKeys == ORDER - 1) {
            splitChild(node, i);
            if (strcmp(key, node->keys[i]) > 0) i++;
        }
        insertNonFull(node->children[i], key);
    }
}

void insert(const char* key) {
    if (!root) {
        root = createNode(1);
        strncpy(root->keys[0], key, KEY_SIZE);
        root->uniHeads[0] = NULL;
        root->numKeys = 1;
        return;
    }
    if (contains(root, key)) return;

    if (root->numKeys == ORDER - 1) {
        Node* newRoot = createNode(0);
        newRoot->children[0] = root;
        splitChild(newRoot, 0);
        root = newRoot;
        treeHeightSeq++;
    }
    insertNonFull(root, key);
}
//csv'yi bolme
void delimeterCSV(char* line, char tokens[][101]) {
    // satır sonu karakterlerini kes
    line[strcspn(line, "\r\n")] = '\0';

    char *p = line;
    char *comma;

    //id = baştan ilk virgüle kadar
    comma = strchr(p, ',');
    *comma = '\0';
    strncpy(tokens[0], p, 100); tokens[0][100] = '\0';
    p = comma + 1;

    //university = p'den bir sonraki virgüle kadar
    comma = strchr(p, ',');
    *comma = '\0';
    strncpy(tokens[1], p, 100); tokens[1][100] = '\0';
    p = comma + 1;

    //department = p'den son virgüle kadar (içindeki virgüller kalır)
    char *last = strrchr(p, ',');
    *last = '\0';
    strncpy(tokens[2], p, 100); tokens[2][100] = '\0';

    // Eğer department tam olarak "..." biçimindeyse, baş ve sondaki tırnakları at
    int len = strlen(tokens[2]);
    if (len >= 2 && tokens[2][0] == '"' && tokens[2][len - 1] == '"') {

        tokens[2][len - 1] = '\0';
        memmove(tokens[2], tokens[2] + 1, len - 1);
    }

    // points = son virgülden sonrası
    strncpy(tokens[3], last + 1, 100); tokens[3][100] = '\0';
}

//bulk load icin csv okuma
int readSortedCsv(const char* filename){
    int count =0;
    char tokens[4][101];
    char line[256];
    FILE* fp = fopen(filename, "r");
    if(!fp) exit(1);


    while(fgets(line,sizeof(line),fp)){


        delimeterCSV(line,tokens);


        records[count] = strdup(tokens[2]);



        strcpy(allRecords[count].id, tokens[0]);
        strcpy(allRecords[count].university, tokens[1]);
        strcpy(allRecords[count].department, tokens[2]);
        allRecords[count].points = atof(tokens[3]);

        count++;







    }
    fclose(fp);

    return count;



}











//sequential icin csv okuma
void readCSV(const char* filename) {
    char tokens[4][101];
    char line[256];

    FILE* fp = fopen(filename, "r");
    if (!fp) exit(1);
    fgets(line, sizeof(line), fp);
    while (fgets(line, sizeof(line), fp)) {
        delimeterCSV(line, tokens);


        insert(tokens[2]);
        clock_t start = clock();
        Node* leaf = findLeaf(root, tokens[2]);
        clock_t end = clock();
        double timeTaken = (double)(end - start) / CLOCKS_PER_SEC;
        avgSeekTimeSeq += timeTaken;


        UniNode* newUni = (UniNode*)malloc(sizeof(UniNode));
        strcpy(newUni->name, tokens[1]);
        newUni->points = atof(tokens[3]);
        newUni->next = NULL;

        for (int i = 0; i < leaf->numKeys; i++) {
            if (strcmp(leaf->keys[i], tokens[2]) == 0) {
                UniNode* head = leaf->uniHeads[i];
                if (!head) leaf->uniHeads[i] = newUni;
                else {
                    while (head->next) head = head->next;
                    head->next = newUni;
                }
                break;
            }
        }

    }
    fclose(fp);
}

void printTree(Node* node, int level) {
    if (!node) return;
    printf("Level %d [", level);
    for (int i = 0; i < node->numKeys; i++) {
        printf("%s", node->keys[i]);
        if (i < node->numKeys - 1) printf(" | ");
    }
    printf("]\n");
    if (!node->isLeaf) for (int i = 0; i <= node->numKeys; i++) printTree(node->children[i], level + 1);
}

void findUniNode(Node* node, const char* key,int rank) {
    int count=1;
    Node* leaf = findLeaf(node, key);
    if (!leaf) return;
    for (int i = 0; i < leaf->numKeys; i++) {
        if (strcmp(leaf->keys[i], key) == 0) {
            UniNode* cur = leaf->uniHeads[i];
            while (cur && count<rank) {
                cur = cur->next;
                count++;
            }
            printf("%s with the base placement score: %5f.\n", cur->name, cur->points);
            return;
        }
    }
    printf("'%s' not found.\n", key);
}











int main(void) {

    int choice=0;

    printf("Please choose a loading option:\n 1 - Sequential Insertion\n2 - Bulk Loading (with external merge sort)\n");
    scanf("%d", &choice);

    if(choice == 1) {

        readCSV("yok_atlas.csv");

        printf("Final B+ Tree:\n");
        printTree(root, 0);
        printf("--------------------------------------\n");
        printf("Print Metrics or Search (1-Metrics, 2-Search): \n");
        int searchChoice;
        scanf("%d", &searchChoice);
        if(searchChoice ==1){
            printf("Sequential Insertion is completed.\n");
            printf("Number of splits: %d\n", splitCountSeq);
            printf("Memory Usage: %f MB\n", (numberOfNodesSeq * sizeof(Node)) / (1024.0 * 1024.0));
            printf("Height of the tree: %d\n", treeHeightSeq+1);


            printf("Average seek time: %.6f sec\n", avgSeekTimeSeq /358 );

        }
        else if(searchChoice == 2) {
            char searchKey[101];
            int rank =0;
            getchar();
            printf("Please enter the department name to search: \n");
            fgets(searchKey, sizeof(searchKey), stdin);
            searchKey[strcspn(searchKey, "\n")] = '\0';
            printf("Please enter the university rank in that department: \n");

            scanf("%d", &rank);
            findUniNode(root, searchKey, rank);
        } else {
            printf("Invalid choice.\n");
        }




    } else if (choice == 2) {
        int totalRuns = 0;
        replacementSelectionSort("yok_atlas.csv", &totalRuns);
        multiWayMerge(totalRuns, "sorted_output.csv");


        int count = readSortedCsv("sorted_output.csv");
        bulkLoad(count, records);
        printf("Final B+ Tree:\n");
       printTree(root, 0);
       printf("--------------------------------------\n");

        printf("Print Metrics or Search (1-Metrics, 2-Search): \n");
        int searchChoice;
        scanf("%d", &searchChoice);
        if(searchChoice ==1){
            printf("Bulk Loading is completed.\n");
            printf("Number of splits: %d\n", splitCountBulk);
            printf("Memory Usage: %f MB\n", (numberOfNodesBulk * sizeof(Node)) / (1024.0 * 1024.0));
            printf("Height of the tree: %d\n", treeHeightBulk+1);


            printf("Average seek time: %.6f sec\n", avgBulkSeekTime() );

        }
        else if(searchChoice == 2) {
            char searchKey[101];
            int rank =0;
            getchar();
            printf("Please enter the department name to search: \n");
            fgets(searchKey, sizeof(searchKey), stdin);
            searchKey[strcspn(searchKey, "\n")] = '\0';
            printf("Please enter the university rank in that department: \n");

            scanf("%d", &rank);
            findUniNode(root, searchKey, rank);
        } else {
            printf("Invalid choice.\n");
        }




    } else {
        printf("Invalid choice. Exiting.\n");
        return 1;
    }




















    return 0;

}
