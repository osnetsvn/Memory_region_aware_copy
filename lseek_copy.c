/*This implementation copies only the valid memory region using lseek with the help of multiple threads

* Lseek copy is more efficient than memcpy because memcpy copies the entire memory region including data and holes

* The CPU affinity is set to avoid the context switching cost

* Copyright 2021 Roja Eswaran, PhD Candidate <reswara1@binghamton.edu>

* Copyright 2021 Prf Kartik Gopalan <kartik@binghamton.edu>

*/



#define _GNU_SOURCE
#include <sys/types.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sched.h>   //cpu_set_t , CPU_SET
#define handle_error_en(en, msg) \
            do { errno = en; perror(msg); exit(EXIT_FAILURE); } while (0)

# define SEEK_DATA      3       /* Seek to next data.  */
# define SEEK_HOLE      4       /* Seek to next hole.  */



void *lseek_cpy_tmpfs (void *arg);



int thread_count;
long total_bytes; 
long chunk; 
struct timeval cpy_start, cpy_end;


int main (int argc, char **argv)
{
	char *str_thread_count;
	int r, trunc; 
	pthread_t *tid;
        int sfd;
	int *thread_index;

	sfd = open ("/mnt/template_ram1/memory", O_RDONLY); // Change the source file name here
	if (sfd == -1 ) {
                  perror("File open error");
	}  
        
	 total_bytes = lseek(sfd, 0, SEEK_END);
	 if(total_bytes == -1)
		perror ("Cannot seek the source file\n");


	str_thread_count = getenv("thread_count"); // The environmental variable should be set before executing the program
	if(str_thread_count == NULL)
		perror("Set the env variable thread_count Eg: export thread_count=2\n");


	thread_count = atoi(str_thread_count); 
	chunk = total_bytes / thread_count;	


	thread_index = (int *) malloc(sizeof(int)*thread_count);
	tid = (pthread_t*) malloc(sizeof(pthread_t)*thread_count);
	if(tid == NULL)
		perror ("Cannot allocate memory for threads\n");


        gettimeofday(&cpy_start, NULL);
	for (int i = 0; i < thread_count; i++) {
		thread_index[i] = i;
		if ((r = pthread_create (&tid [i], NULL, lseek_cpy_tmpfs, (void*)&thread_index[i])) != 0) {

			perror("Cannot create multiple threads\n");
		}
	}


	// Wait for threads to terminate
	for (int i = 0; i < thread_count; i++) {
		if ((r = pthread_join (tid [i], NULL)) != 0) {

			perror("Cannot join multiple threads\n");
		}
	}
        gettimeofday(&cpy_end, NULL);

        printf("Total time : %ld micro seconds\n",((cpy_end.tv_sec - cpy_start.tv_sec)*1000000) + cpy_end.tv_usec-cpy_start.tv_usec);
         

	close(sfd);
	exit (0);
}


/* This function skips the holes and copy only the valid memory region to destination file using lseek and write */

void *lseek_cpy_tmpfs(void *arg){
     
        
     
        int sfd = open ("/mnt/template_ram1/memory", O_RDONLY);
        int dfd = open ("/mnt/template_ram3/memory", O_CREAT | O_RDWR, 0644); // Change the destination file name to be copied here
	if (sfd == -1 || dfd == -1) {
                  perror("File open error");
	}

        cpu_set_t cpuset;
        int tid = *(int *)(arg);
        long off_begin, off_end, byte_length, rw_bytes;
	long offset = tid * chunk;
	char *buffer = (char *) malloc(1024L*1024L*1024L);
        long total_data = 0;       
        long exceed_range = 0;


        CPU_ZERO(&cpuset);
        CPU_SET((tid+1), &cpuset);
       

        int s = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
        if (s != 0)
                   handle_error_en(s, "pthread_setaffinity_np");

        
        
        s = pthread_getaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
        if (s != 0)
                handle_error_en(s, "pthread_getaffinity_np");


        //printf("Set returned by pthread_getaffinity_np() contained:\n");
        for (int j = 0; j < CPU_SETSIZE; j++)
                if (CPU_ISSET(j, &cpuset))
                        printf("CPU %d TID:%d\n", j, tid);

        
        if(total_bytes % thread_count)
         	if(tid == thread_count-1)
                	chunk = total_bytes - ((thread_count-1) * chunk);

	long end_range = chunk + offset;
        
     

         do{
                 off_begin = lseek(sfd, offset, SEEK_DATA);
                 off_end = lseek(sfd, off_begin, SEEK_HOLE);
                 if(off_begin == -1 || off_end ==-1)
                         perror("Data/hole seek error\n");
                 
                 if(off_begin >= end_range){
                        break;
                 }


                 byte_length = off_end - off_begin;
                 exceed_range = off_begin + byte_length;
                

                 if(exceed_range >= end_range){
                        byte_length = end_range - off_begin;
                 }
                        
                 
                 
                 
                 total_data = total_data + byte_length;


                 lseek(sfd, off_begin, SEEK_SET);
                 lseek(dfd, off_begin, SEEK_SET);



                 rw_bytes = read(sfd, buffer, byte_length);
                 rw_bytes = write(dfd, buffer, rw_bytes);
                 memset(buffer,0,rw_bytes);


                 offset = off_end;
                 lseek(sfd, offset, SEEK_SET);
                 lseek(dfd, offset, SEEK_SET);

         }while(offset  < end_range );
        
        close(sfd);
        close(dfd);
        return NULL;

}
