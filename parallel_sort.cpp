/**
 * @file    parallel_sort.cpp
 * @author  Patrick Flick <patrick.flick@gmail.com>
 * @brief   Implements the parallel, distributed sorting function.
 *
 * Copyright (c) 2014 Georgia Institute of Technology. All Rights Reserved.
 */

#include "parallel_sort.h"

// C std lib
#include <stdint.h>
#include <cstdlib>
#include <stdio.h>
#include <stdlib.h>

// for in/output
#include <iostream>
#include <fstream>
#include <iterator>
#include <vector>
#include <algorithm>

// own includes
#include "io.h"
#include "utils.h"

int compare (const void * a, const void * b)
{
  return ( *(int*)a - *(int*)b );
}

void swap(int* a, int* b)
{
    int t = *a;
    *a = *b;
    *b = t;
}

int quickSort(int *arr, int low, int high, int pivot)
{
    if (low <= high)
    {
        int i = (low - 1);  // Index of smaller element
 
    	for (int j = low; j <= high; j++)
    	{
            if (*(arr+j) <= pivot)
            {
                i++;    // increment index of smaller element
                swap(arr+i, arr+j);
            }
        }
        return (i+1);
    }
    return 0;
}


// implementation of your parallel sorting
void parallel_sort(int * begin, int* end, MPI_Comm comm) {

    int rank, p;

    MPI_Comm_rank(comm, &rank);
    MPI_Comm_size(comm, &p);

    int m_loc = (end - begin);

    if (m_loc == 0) return;

    if(rank == 0) printf("m_local is %d\n",m_loc);
    if(p==1)
    {
        qsort (begin, m_loc, sizeof(int), compare);
        for(int i=0;i<m_loc;i++) printf("after sort rank %d position %d num %d \n",rank,i,*(begin+i));
        MPI_Barrier (comm);
        if(rank == 0) printf("\n"); 
        return;
    }

    int *sizesarray = (int*) malloc(p*sizeof(int));
    MPI_Barrier (comm);
    MPI_Allgather(&m_loc, 1, MPI_INT, sizesarray, 1, MPI_INT, comm);
    MPI_Barrier (comm);

    int totalnumbers = 0;
    for(int i=0; i<p; i++) totalnumbers += sizesarray[i];

    srand(totalnumbers); //For now, assume everyone has same number of elements

    int k = rand()%(totalnumbers); //For now, assume everyone has same number of elements
    //printf("rank is %d k is %d\n",rank ,k);

    int pivot;
    int root_for_pivot = 0;
    int pivot_count = 0;
    if(k == 0)
    {
        root_for_pivot = 0;
        pivot = *(begin);
    }
    else
    {
        int n1 = 0;
        while(pivot_count <= k)
        {
             if(pivot_count + sizesarray[n1] > k)
             {
                  root_for_pivot = n1;
                  pivot = *(begin + k - pivot_count);
                  break;
             }
             else
             {
                  pivot_count += sizesarray[n1];
                  n1++;
             }
        }
    }
    MPI_Barrier (comm);
    MPI_Bcast(&pivot,1,MPI_INT,root_for_pivot,comm);

    if(rank==1) printf("pivot is %d\n",pivot);
    MPI_Barrier (comm);

    //for(int i=0;i<m_loc;i++) printf("before sorting rank %d num %d \n",rank,*(begin+i));

    int m1=0;
    m1 = quickSort(begin, 0, m_loc-1, pivot);

    for(int i=0;i<m_loc;i++) printf(" rank %d num %d \n",rank,*(begin+i));

    //if(rank==1) printf("\n Index of element after pivot is %d\n",m1);
    int m2 = m_loc - m1;

    int *mless = (int*) malloc(p*sizeof(int));
    int *mmore = (int*) malloc(p*sizeof(int));

    MPI_Barrier (comm);
    MPI_Allgather(&m1, 1, MPI_INT, mless, 1, MPI_INT, comm);
    MPI_Allgather(&m2, 1, MPI_INT, mmore, 1, MPI_INT, comm);
    MPI_Barrier (comm);

    if(rank==1) for(int i=0; i<p; i++) printf("rank is %d m1 is %d m2 is %d\n",i ,*(mless+i) ,*(mmore+i));

    int ltpivot=0, gtpivot=0;
    for(int i=0; i<p; i++) ltpivot += *(mless+i);
    for(int i=0; i<p; i++) gtpivot += *(mmore+i);

    // pa : processors for lesser half
    int pa = (ltpivot * p/(ltpivot + gtpivot));
    int pb = p - pa;

    if(pa == 0) {pa=1; pb--;}
    else if(pa == p) {pa--;pb=1;}

    if(rank==1) printf("pa is %d, pb is %d\n",pa ,pb);
if(rank==1) printf("ltpivot is %d, gtpivot is %d\n",ltpivot ,gtpivot);

    int *sizesfornextround = (int*) malloc(p*sizeof(int));
    int ltpivot_rem = ltpivot % pa;
    if(rank==1) printf("ltpivot_rem is %d\n",ltpivot_rem);
    for(int i=0; i<pa; i++) sizesfornextround[i] = ltpivot/pa;
    for(int i=0; i<ltpivot_rem; i++) (sizesfornextround[i])++;

    int gtpivot_rem = gtpivot % pb;
    if(rank==1) printf("gtpivot_rem is %d\n",gtpivot_rem);
    for(int i=pa; i<p; i++) sizesfornextround[i] = gtpivot/pb;
    for(int i=pa; i<pa+gtpivot_rem; i++) (sizesfornextround[i])++;
    MPI_Barrier (comm);
    if(rank==1) for(int i=0; i<p; i++) printf("rank is %d sizes for next round is %d\n",i ,sizesfornextround[i]);

    int thisnextroundsize = sizesfornextround[rank];

    //std::vector<int> gatherarray = (int*) malloc(sizesfornextround[rank]*sizeof(int));
    int *gatherarray = (int*) malloc(sizesfornextround[rank]*sizeof(int)); //The array to be created for the next round
    //for(int i=0; i<m_loc; i++) gatherarray[i] = *(begin+i);
    //for(int i=0; i<sizesfornextround[rank]; i++) gatherarray[i] = 0;
    MPI_Barrier (comm);

    if(1)
    {
        int *sendcount = (int*) malloc(p*sizeof(int));
        int *send_displ = (int*) malloc(p*sizeof(int));
        //int send_sum = mless[rank]; //used for send_disp
        int send_sum = 0;
        int *recvcount = (int*) malloc(p*sizeof(int));
        int *recv_displ = (int*) malloc(p*sizeof(int));
        //int recv_sum = mless[rank]; //used for send_disp
        int recv_sum = 0;
        int tosend=mless[rank],sent=0;
        int torecv=sizesfornextround[rank],received=0;
        if(rank >= pa) torecv = 0;
        for(int i=0; i<p; i++) {sendcount[i]=0; send_displ[i] = 0; recvcount[i]=0; recv_displ[i] = 0;}
        for(int j=0;j<rank;j++) sent += mless[j];
        for(int j=0;j<rank;j++) received += sizesfornextround[j];
        for(int i=0; i<p; i++)
        {
            if(sent >= sizesfornextround[i])
            {
                 sendcount[i] = 0;
                 send_displ[i] = 0;
                 sent -= sizesfornextround[i];
            }
            else if(sent > 0 && tosend > 0)
            {
                 if(tosend > sizesfornextround[i] - sent) 
                 {
                      sendcount[i] = sizesfornextround[i] - sent;
                      send_displ[i] = send_sum;
                      send_sum += sendcount[i];
                      tosend -= sendcount[i];
                 }
                 else
                 {
                      sendcount[i] = tosend;
                      send_displ[i] = send_sum;
                      send_sum += sendcount[i];
                      tosend = 0;
                 }
                 sent = 0;
            }
            else if(tosend > 0)
            {
                 if(tosend > sizesfornextround[i])
                 {
                      sendcount[i] = sizesfornextround[i];
                      send_displ[i] = send_sum;
                      send_sum += sendcount[i];
                      tosend -= sizesfornextround[i];
                 }
                 else
                 {
                      sendcount[i] = tosend;
                      send_displ[i] = send_sum;
                      send_sum += sendcount[i];
                      tosend = 0;
                 }
            }
            // receive counts and displacements
            if(received >= mless[i])
            {
                 recvcount[i] = 0;
                 recv_displ[i] = 0;
                 received -= mless[i];
            }
            else if(received > 0 && torecv > 0)
            {
                 if(torecv > mless[i] - received) 
                 {
                      recvcount[i] = mless[i] - received;
                      recv_displ[i] = recv_sum;
                      recv_sum += recvcount[i];
                      torecv -= recvcount[i];
                 }
                 else
                 {
                      recvcount[i] = torecv;
                      recv_displ[i] = recv_sum;
                      recv_sum += recvcount[i];
                      torecv = 0;
                 }
                 received = 0;
            }
            else if(torecv > 0)
            {
                 if(torecv > mless[i])
                 {
                      recvcount[i] = mless[i];
                      recv_displ[i] = recv_sum;
                      recv_sum += recvcount[i];
                      torecv -= mless[i];
                 }
                 else
                 {
                      recvcount[i] = torecv;
                      recv_displ[i] = recv_sum;
                      recv_sum += recvcount[i];
                      torecv = 0;
                 }
            }
        }
        MPI_Barrier (comm);
        //for(int j=0; j<p; j++)
            //printf("rank is %d, going to %d, sendcount is %d send_disp is %d, receivecount is %d, recv_displ is %d\n",rank ,j, sendcount[j], send_displ[j], recvcount[j], recv_displ[j]);
        MPI_Alltoallv(begin, sendcount, send_displ, MPI_INT, gatherarray, recvcount, recv_displ, MPI_INT, comm);
        MPI_Barrier (comm);



        send_sum = 0;
        recv_sum = 0;
        tosend=mmore[rank];
        sent=0;
        torecv=sizesfornextround[rank];
        received=0;
        if(rank < pa) torecv = 0;
        for(int i=0; i<p; i++) {sendcount[i]=0; send_displ[i] = 0; recvcount[i]=0; recv_displ[i] = 0;}
        for(int j=0;j<rank;j++) sent += mmore[j];
        for(int j=pa;j<rank;j++) received += sizesfornextround[j];
        int *send2 = (int*) malloc(p*sizeof(int));
        for(int i=0; i<p; i++) 
            if(i<pa) send2[i]=0;
            else send2[i] = sizesfornextround[i];
        for(int i=0; i<p; i++)
        {
            if(sent >= send2[i])
            {
                 sendcount[i] = 0;
                 send_displ[i] = 0;
                 sent -= send2[i];
            }
            else if(sent > 0 && tosend > 0)
            {
                 if(tosend > send2[i] - sent) 
                 {
                      sendcount[i] = send2[i] - sent;
                      send_displ[i] = send_sum;
                      send_sum += sendcount[i];
                      tosend -= sendcount[i];
                 }
                 else
                 {
                      sendcount[i] = tosend;
                      send_displ[i] = send_sum;
                      send_sum += sendcount[i];
                      tosend = 0;
                 }
                 sent = 0;
            }
            else if(tosend > 0)
            {
                 if(tosend > send2[i])
                 {
                      sendcount[i] = send2[i];
                      send_displ[i] = send_sum;
                      send_sum += sendcount[i];
                      tosend -= send2[i];
                 }
                 else
                 {
                      sendcount[i] = tosend;
                      send_displ[i] = send_sum;
                      send_sum += sendcount[i];
                      tosend = 0;
                 }
            }
            // receive counts and displacements
            if(received >= mmore[i])
            {
                 recvcount[i] = 0;
                 recv_displ[i] = 0;
                 received -= mmore[i];
            }
            else if(received > 0 && torecv > 0)
            {
                 if(torecv > mmore[i] - received) 
                 {
                      recvcount[i] = mmore[i] - received;
                      recv_displ[i] = recv_sum;
                      recv_sum += recvcount[i];
                      torecv -= recvcount[i];
                 }
                 else
                 {
                      recvcount[i] = torecv;
                      recv_displ[i] = recv_sum;
                      recv_sum += recvcount[i];
                      torecv = 0;
                 }
                 received = 0;
            }
            else if(torecv > 0)
            {
                 if(torecv > mmore[i])
                 {
                      recvcount[i] = mmore[i];
                      recv_displ[i] = recv_sum;
                      recv_sum += recvcount[i];
                      torecv -= mmore[i];
                 }
                 else
                 {
                      recvcount[i] = torecv;
                      recv_displ[i] = recv_sum;
                      recv_sum += recvcount[i];
                      torecv = 0;
                 }
            }
        }
        MPI_Barrier (comm);
        //if(rank==1) printf("after pb calculation\n");
        MPI_Barrier (comm);
        //for(int j=0; j<p; j++)
            //printf("rank is %d, going to %d, sendcount is %d send_disp is %d, receivecount is %d, recv_displ is %d\n",rank ,j, sendcount[j], send_displ[j], recvcount[j], recv_displ[j]);
        MPI_Barrier (comm);
        MPI_Alltoallv((begin+mless[rank]), sendcount, send_displ, MPI_INT, gatherarray, recvcount, recv_displ, MPI_INT, comm);
        MPI_Barrier (comm);
    }


    for(int i=0;i<thisnextroundsize;i++) printf("after all to all rank %d num %d \n",rank,*(gatherarray+i));
    //for(int i=0; i<m_loc; i++) *(begin+i) = *(gatherarray+i);


    //std::vector<int> local_elements;
    //std::vector<int> input_vector;

    int colour = (rank < pa) ? 0 : 1;
    MPI_Barrier (comm);
    MPI_Comm newcomm;
    MPI_Comm_split(comm, colour, rank, &newcomm);
    parallel_sort(gatherarray, gatherarray + thisnextroundsize, newcomm);
    MPI_Barrier (comm);

    if(1)
    {
        int *sendcount = (int*) malloc(p*sizeof(int));
        int *send_displ = (int*) malloc(p*sizeof(int));
        int send_sum = 0;
        int *recvcount = (int*) malloc(p*sizeof(int));
        int *recv_displ = (int*) malloc(p*sizeof(int));
        int recv_sum = 0;

        int tosend=sizesfornextround[rank],sent=0;
        int torecv=m_loc,received=0;

        for(int i=0; i<p; i++) {sendcount[i]=0; send_displ[i] = 0; recvcount[i]=0; recv_displ[i] = 0;}
        for(int j=0;j<rank;j++) sent += sizesfornextround[j];
        for(int j=0;j<rank;j++) received += sizesarray[j];

        for(int i=0; i<p; i++)
        {
            //send counts
            if(sent >= sizesarray[i])
            {
                 sendcount[i] = 0;
                 send_displ[i] = 0;
                 sent -= sizesarray[i];
            }
            else if(sent > 0 && tosend > 0)
            {
                 if(tosend > sizesarray[i] - sent) 
                 {
                      sendcount[i] = sizesarray[i] - sent;
                      send_displ[i] = send_sum;
                      send_sum += sendcount[i];
                      tosend -= sendcount[i];
                      //sent -= sendcount[i];
                      //if(sent < 0) sent = 0;
                 }
                 else
                 {
                      sendcount[i] = tosend;
                      send_displ[i] = send_sum;
                      send_sum += sendcount[i];
                      tosend = 0;
                      //sent -= sendcount[i];
                 }
                 sent = 0;
            }
            else if(tosend > 0)
            {
                 if(tosend > sizesarray[i])
                 {
                      sendcount[i] = sizesarray[i];
                      send_displ[i] = send_sum;
                      send_sum += sendcount[i];
                      tosend -= sizesarray[i];
                 }
                 else
                 {
                      sendcount[i] = tosend;
                      send_displ[i] = send_sum;
                      send_sum += sendcount[i];
                      tosend = 0;
                 }
            }

            //receive counts
            if(received >= sizesfornextround[i])
            {
                 recvcount[i] = 0;
                 recv_displ[i] = 0;
                 received -= sizesfornextround[i];
            }
            else if(received > 0 && torecv > 0)
            {
                 if(torecv > sizesfornextround[i] - received) //dont change
                 {
                      recvcount[i] = sizesfornextround[i] - received;
                      recv_displ[i] = recv_sum;
                      recv_sum += recvcount[i];
                      torecv -= recvcount[i];
                      //received -= recvcount[i];
                 }
                 else
                 {
                      recvcount[i] = torecv;
                      recv_displ[i] = recv_sum;
                      recv_sum += recvcount[i];
                      torecv = 0;
                      //received -= recvcount[i];
                 }
                 received = 0;
            }
            else if(torecv > 0)
            {
                 if(torecv > sizesfornextround[i])
                 {
                      recvcount[i] = sizesfornextround[i];
                      recv_displ[i] = recv_sum;
                      recv_sum += recvcount[i];
                      torecv -= sizesfornextround[i];
                 }
                 else
                 {
                      recvcount[i] = torecv;
                      recv_displ[i] = recv_sum;
                      recv_sum += recvcount[i];
                      torecv = 0;
                 }
            }
        }
        MPI_Barrier (comm);
        //for(int i=0; i<p; i++) printf("Rank is %d. to processor %d, sendcount is %d, send_displ is %d\n",rank, i, sendcount[i], send_displ[i]);
        MPI_Barrier (comm);
        //for(int i=0; i<p; i++) printf("Rank is %d. to processor %d, recvcount is %d, recv_displ is %d\n",rank, i, recvcount[i], recv_displ[i]);
        MPI_Barrier (comm);
        MPI_Alltoallv(gatherarray, sendcount, send_displ, MPI_INT, begin, recvcount, recv_displ, MPI_INT, comm);
        MPI_Barrier (comm);
    }


    //std::vector<int> sorted_elements;
    //sorted_elements = gather_vectors(&&gatherarray, comm);
    //gather_array = sorted_elements;
    for(int i=0;i<m_loc;i++) printf("after sort rank %d num %d \n",rank,*(begin+i));
    MPI_Barrier (comm);
    if(rank == 0) printf("\n");
    MPI_Barrier (comm);
}


/*********************************************************************
 *             Implement your own helper functions here:             *
 *********************************************************************/

// ...
