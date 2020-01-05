package com.github.fevernova.framework.common.structure.queue;


import java.util.AbstractQueue;
import java.util.Iterator;


public class LinkedQueue<E extends LinkedObject<E>> extends AbstractQueue<E> {


    private int size;

    private E first;

    private E last;


    @Override public int size() {

        return this.size;
    }


    @Override public boolean offer(E e) {

        e.setNext(null);
        if (this.first == null) {
            this.first = e;
            this.last = e;
        } else {
            this.last.setNext(e);
            this.last = e;
        }
        this.size++;
        return true;
    }


    @Override public E poll() {

        E result = this.first;
        if (this.size < 2) {
            this.size = 0;
            this.first = null;
            this.last = null;
        } else {
            this.size--;
            this.first = this.first.getNext();
        }
        return result;
    }


    @Override public E peek() {

        return this.first;
    }


    @Override public void clear() {

        this.size = 0;
        this.first = null;
        this.last = null;
    }


    @Override public boolean remove(Object o) {

        if (this.size == 0) {
            return false;
        }

        E cur = this.first;
        if (cur == o) {
            poll();
            return true;
        }

        while (cur.getNext() != null) {
            if (cur.getNext() == o) {
                this.size--;
                if (this.last == cur.getNext()) {
                    this.last = cur;
                }
                cur.setNext(cur.getNext().getNext());
                return true;
            }
            cur = cur.getNext();
        }
        return false;
    }


    public E findAndRemove(FindFunction<E> findFunction) {

        if (this.size == 0) {
            return null;
        }

        E cur = this.first;
        if (findFunction.find(cur)) {
            poll();
            return cur;
        }

        while (cur.getNext() != null) {
            if (findFunction.find(cur.getNext())) {
                this.size--;
                if (this.last == cur.getNext()) {
                    this.last = cur;
                }
                E r = cur.getNext();
                cur.setNext(r.getNext());
                return r;
            }
            cur = cur.getNext();
        }
        return null;
    }


    @Override public Iterator<E> iterator() {

        return new Itr();
    }


    private class Itr implements Iterator<E> {


        E e = first;


        @Override public boolean hasNext() {

            return e != null;
        }


        @Override public E next() {

            return e = e.getNext();
        }
    }

}
