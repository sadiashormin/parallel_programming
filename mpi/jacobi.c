#include "mpi.h"               /* required MPI library */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <sys/time.h>
#define MASTER 0               /* taskid of first task */
#define FROM_MASTER 1          /* setting a message type */
#define FROM_WORKER 2          /* setting a message type */

float fault = 1.0;
float *srcData,*destData;

float *subSrcData,*subDestData,*subTemp;

int main(argc,argv)
int argc;
char *argv[];
{
    int numtasks,              /* number of tasks in partition */
        taskid,                /* a task identifier */
        numworkers,            /* number of worker tasks */
        source,                /* task id of message source */
        dest,                  /* task id of message destination */
        mtype,                 /* message type */
        rows,                  /* rows of matrix A sent to each worker */
        averow, extra, offset, /* determines rows sent to each worker */
        i,j,rc;

    int maxRows;

    struct timeval tv1, tv2;
     int sizeI = 16384, sizeJ =16384;

    float difference = 10000;
    int   running = 1;

    MPI_Status status;

    rc = MPI_Init(&argc,&argv);

    rc|= MPI_Comm_size(MPI_COMM_WORLD,&numtasks);

    rc|= MPI_Comm_rank(MPI_COMM_WORLD,&taskid);

    if (rc != 0)
       printf ("error initializing MPI and obtaining task ID info\n");

    numworkers = numtasks-1;

    if (taskid == MASTER)
    {
        printf("Number of worker tasks = %d\n",numworkers);
        FILE *fp = fopen("./matrix.dat", "rb" );

        gettimeofday(&tv1, NULL);

        if (fp)
        {
            printf("start reading data\n");
            srcData = malloc(sizeI * sizeJ * sizeof(float *));

            // read the values from the file
            fread(srcData,sizeof(float),sizeI*sizeJ,fp);

            // padding the boundary

            for (i=0; i<sizeI; i++) {
                srcData[i*sizeJ] = 0;
                srcData[i*sizeJ+sizeJ-1]=0;
            }
            for (j=0; j<sizeJ; j++) {
                srcData[j] = 0;
                srcData[(sizeI-1)*sizeJ+j]=0;
            }

            destData = malloc(sizeI * sizeJ * sizeof(float *));

            for (i=0; i<sizeI; i++) {
                destData[i*sizeJ] = 0;
                destData[i*sizeJ+sizeJ-1]=0;
            }
            for (j=0; j<sizeJ; j++) {
                destData[j] = 0;
                destData[(sizeI-1)*sizeJ+j]=0;
            }
            // close the file
            fclose(fp);
        }else{
            printf("cannot open file\n");
            return 1;
        }

         averow = sizeI/numworkers;
         extra =  sizeI%numworkers;
         offset = 0;
         mtype = FROM_MASTER;
         for (dest=1; dest<=numworkers; dest++)
         {
             rows = (dest <= extra)?averow+1:averow;
             int newRows = (dest==numworkers)?rows:rows+2;

             MPI_Send(&offset, 1, MPI_INT, dest, mtype, MPI_COMM_WORLD);

             MPI_Send(&newRows, 1, MPI_INT, dest, mtype, MPI_COMM_WORLD);

             MPI_Send(&srcData[offset*sizeJ], newRows*sizeJ, MPI_FLOAT, dest, mtype,MPI_COMM_WORLD);
               printf("%d Send rows from offset: %d, rows: %d to slave %d \n", taskid,offset,newRows, dest);

             offset = offset + rows;
         }

         while(difference > fault)
         {
             difference = 0;
             mtype = FROM_WORKER;
             for (i=1; i<=numworkers; i++)
             {
                 float sub_diff = 1000;
                 source = i;
                 MPI_Recv(&sub_diff, 1, MPI_FLOAT, source, mtype, MPI_COMM_WORLD,&status);
                 if(sub_diff > difference)
                     difference = sub_diff;
             }
             printf("Difference > fault and continue running\n");
             if( difference > fault)
             {
                 mtype = FROM_MASTER;
                 for (dest=1; dest<=numworkers; dest++)
                 {
                     MPI_Send(&running, 1, MPI_INT, dest, mtype, MPI_COMM_WORLD);
                 }
             }
 mtype = FROM_MASTER;
         running = 0;
         for (dest=1; dest<=numworkers; dest++)
         {
             MPI_Send(&running, 1, MPI_INT, dest, mtype, MPI_COMM_WORLD);
         }
         printf("Sending terminate signal to all worker\n");

         /*Below is test code for communication*/
         mtype = FROM_WORKER;
         for (i=1; i<=numworkers; i++)
         {
             source = i;

             MPI_Recv(&offset, 1, MPI_INT, source, mtype, MPI_COMM_WORLD,&status);

             MPI_Recv(&rows, 1, MPI_INT, source, mtype, MPI_COMM_WORLD, &status);

             MPI_Recv(&destData[offset*sizeJ], rows*sizeJ, MPI_FLOAT, source, mtype,MPI_COMM_WORLD, &status);
             /*
             for(i=0;i<Rows;i++)
             {
                 MPI_Send(&srcData[offset+i][0], sizeJ, MPI_FLOAT, dest, mtype,MPI_COMM_WORLD);
             }
             */
             printf("%d Get from offset: %d , %d rows from slave %d\n", taskid, offset,rows, source);
         }


        gettimeofday(&tv2, NULL);
        // print the time consumed
        printf("The time is %ld sec and %ld millisec\n", (tv2.tv_sec - tv1.tv_sec), (tv2.tv_usec - tv1.tv_usec));

        printf ("Master node finished\n");

    }
 if (taskid > MASTER)
    {
        maxRows = (sizeI + numworkers -1)/numworkers;

        int allocRows = maxRows + 2;

        subSrcData  = malloc(allocRows * sizeJ * sizeof(float));

        subDestData = malloc(allocRows * sizeJ * sizeof(float));

        printf("Malloc buffer on slave: %d, src: %u, dst: %u\n", allocRows,subSrcData,subDestData);

        mtype = FROM_MASTER;

        MPI_Recv(&offset, 1, MPI_INT, MASTER, mtype, MPI_COMM_WORLD, &status);

        MPI_Recv(&rows, 1, MPI_INT, MASTER, mtype, MPI_COMM_WORLD, &status);

        MPI_Recv(subSrcData, rows*sizeJ, MPI_FLOAT, MASTER, mtype, MPI_COMM_WORLD,&status);

        printf("%d Get rows from offset: %d, %d rows from master\n", taskid, offset,rows);

        mtype = FROM_WORKER;

        int newOffset = offset+1;
        int newRows   = rows - 2;

        printf("To copy data, offset:%d, rows:%d , src: %u, dest: %u\n",newOffset, newRows,&subSrcData[sizeJ],&subDestData[sizeJ]);

        //check if need to continue.
        memcpy(&subDestData[0],&subSrcData[0],rows*sizeJ*sizeof(float));

        MPI_Request req1,req2;
        MPI_Status  wStatus;
        while(1)
        {
            difference = 0;
            for(i=1; i<rows-1; i++)
            {
                for(j=1; j<sizeJ-1; j++)
                {
                    subDestData[i*sizeJ+j] = (subSrcData[(i-1)*sizeJ + j-1] + subSrcData[(i-1)*sizeJ + j] + subSrcData[(i-1)*sizeJ + j+1] + subSrcData[i*sizeJ + j-1]
                                    + subSrcData[i*sizeJ + j+1] + subSrcData[(i+1)*sizeJ + j-1] + subSrcData[(i+1)*sizeJ + j] + subSrcData[(i+1)*sizeJ + j+1]) / 8;
                    if (fabs(subDestData[i*sizeJ + j] - subSrcData[i*sizeJ + j]) > difference)
                    {
                        difference = fabs(subDestData[i*sizeJ + j] - subSrcData[i*sizeJ + j]);
                    }
                }
            }

            MPI_Isend(&difference, 1, MPI_FLOAT, MASTER, mtype, MPI_COMM_WORLD,&req1);
            MPI_Recv(&running, 1, MPI_INT, MASTER, FROM_MASTER, MPI_COMM_WORLD, &status);
            MPI_Wait(&req1,&wStatus);
            if(!running)
            {
                printf("Get the final result and exit from %d\n",taskid);
                break;
            }else{
                printf("Continue computing the result in  %d\n",taskid);
            }

if(taskid == 1)
            {
                MPI_Isend(subDestData+sizeJ*(rows-2),sizeJ, MPI_FLOAT, taskid+1, mtype, MPI_COMM_WORLD,&req1);
                MPI_Recv(subDestData+sizeJ*(rows-1),sizeJ, MPI_FLOAT, taskid+1, mtype, MPI_COMM_WORLD,&status);
                MPI_Wait(&req1,&wStatus);
            }else if(taskid == numworkers) {
                MPI_Isend(subDestData+sizeJ,sizeJ, MPI_FLOAT, taskid-1, mtype, MPI_COMM_WORLD,&req1);
                MPI_Recv(subDestData,  sizeJ, MPI_FLOAT, taskid-1, mtype, MPI_COMM_WORLD,&status);
                MPI_Wait(&req1,&wStatus);
            }else {
                MPI_Isend(subDestData+sizeJ,sizeJ, MPI_FLOAT, taskid-1, mtype, MPI_COMM_WORLD,&req1);
                MPI_Recv(subDestData,  sizeJ, MPI_FLOAT, taskid-1, mtype, MPI_COMM_WORLD,&status);
                MPI_Isend(subDestData+sizeJ*(rows-2),sizeJ, MPI_FLOAT, taskid+1, mtype, MPI_COMM_WORLD,&req2);
                MPI_Recv(subDestData+sizeJ*(rows-1),sizeJ, MPI_FLOAT, taskid+1, mtype, MPI_COMM_WORLD,&status);
                MPI_Wait(&req1,&wStatus);
                MPI_Wait(&req2,&wStatus);
            }
            subTemp = subSrcData;
            subSrcData = subDestData;
            subDestData = subTemp;

            printf("next iteration:%lf, %lf, %lf in slave %d\n", difference, subDestData[1*sizeJ + 1], subSrcData[1*sizeJ + 1],taskid);
        }


        MPI_Send(&newOffset, 1, MPI_INT, MASTER, mtype, MPI_COMM_WORLD);

        MPI_Send(&newRows, 1, MPI_INT, MASTER, mtype, MPI_COMM_WORLD);

        MPI_Send(subDestData+sizeJ, newRows*sizeJ, MPI_FLOAT, MASTER, mtype, MPI_COMM_WORLD);

        printf("%d Send rows:from offset: %d , %d rows to master\n", taskid, newOffset, newRows);

        printf("Worker %d is finished\n",taskid);

    }
   free(srcData);
    free(destData);
    free(subSrcData);
    free(subDestData);
    MPI_Finalize();
}

