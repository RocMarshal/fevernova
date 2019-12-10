package com.github.fevernova.framework.common.structure.list;


import java.util.*;
import java.util.function.Consumer;


public class FixedList<E> implements List<E> {


    private Object[] elementData;


    public FixedList(int capacity) {

        this.elementData = new Object[capacity];
    }


    @Override
    public int size() {

        return elementData.length;
    }


    @Override
    public boolean isEmpty() {

        return false;
    }


    @Override
    public boolean contains(Object o) {

        return indexOf(o) >= 0;
    }


    @Override
    public Iterator<E> iterator() {

        return new Itr();
    }


    @Override
    public Object[] toArray() {

        return Arrays.copyOf(elementData, elementData.length);
    }


    @Override
    public <T> T[] toArray(T[] a) {

        if (a.length < elementData.length)
            return (T[]) Arrays.copyOf(elementData, elementData.length, a.getClass());
        System.arraycopy(elementData, 0, a, 0, elementData.length);
        return a;
    }


    @Override
    public boolean add(E e) {

        throw new RuntimeException("FixedList Not Support");
    }


    @Override
    public boolean remove(Object o) {

        if (o == null) {
            return true;
        } else {
            for (int index = 0; index < elementData.length; index++)
                if (o.equals(elementData[index])) {
                    elementData[index] = null;
                    return true;
                }
        }
        return false;
    }


    @Override
    public boolean containsAll(Collection<?> c) {

        throw new RuntimeException("FixedList Not Support");
    }


    @Override
    public boolean addAll(Collection<? extends E> c) {

        throw new RuntimeException("FixedList Not Support");
    }


    @Override
    public boolean addAll(int index, Collection<? extends E> c) {

        throw new RuntimeException("FixedList Not Support");
    }


    @Override
    public boolean removeAll(Collection<?> c) {

        throw new RuntimeException("FixedList Not Support");
    }


    @Override
    public boolean retainAll(Collection<?> c) {

        throw new RuntimeException("FixedList Not Support");
    }


    @Override
    public void clear() {

        for (int i = 0; i < elementData.length; i++)
            elementData[i] = null;
    }


    @Override
    public E get(int index) {

        return elementData(index);
    }


    @Override
    public E set(int index, E element) {

        E e = elementData(index);
        elementData[index] = element;
        return e;
    }


    @Override
    public void add(int index, E element) {

        set(index, element);
    }


    @Override
    public E remove(int index) {

        E e = elementData(index);
        elementData[index] = null;
        return e;
    }


    E elementData(int index) {

        return (E) elementData[index];
    }


    @Override
    public int indexOf(Object o) {

        if (o == null) {
            for (int i = 0; i < elementData.length; i++)
                if (elementData[i] == null)
                    return i;
        } else {
            for (int i = 0; i < elementData.length; i++)
                if (o.equals(elementData[i]))
                    return i;
        }
        return -1;
    }


    @Override
    public int lastIndexOf(Object o) {

        if (o == null) {
            for (int i = elementData.length - 1; i >= 0; i--)
                if (elementData[i] == null)
                    return i;
        } else {
            for (int i = elementData.length - 1; i >= 0; i--)
                if (o.equals(elementData[i]))
                    return i;
        }
        return -1;
    }


    @Override
    public ListIterator<E> listIterator() {

        throw new RuntimeException("FixedList Not Support");
    }


    @Override
    public ListIterator<E> listIterator(int index) {

        throw new RuntimeException("FixedList Not Support");
    }


    @Override
    public List<E> subList(int fromIndex, int toIndex) {

        throw new RuntimeException("FixedList Not Support");
    }


    private class Itr implements Iterator<E> {


        int cursor;       // index of next element to return


        Itr() {

        }


        public boolean hasNext() {

            return cursor != elementData.length;
        }


        @SuppressWarnings("unchecked")
        public E next() {

            int i = cursor;
            if (i >= elementData.length)
                throw new NoSuchElementException();
            Object[] elementData = FixedList.this.elementData;
            return (E) elementData[cursor++];
        }


        public void remove() {

            throw new RuntimeException("FixedList Not Support");
        }


        @Override
        @SuppressWarnings("unchecked")
        public void forEachRemaining(Consumer<? super E> consumer) {

            Objects.requireNonNull(consumer);
            final int size = FixedList.this.elementData.length;
            int i = cursor;
            if (i >= size) {
                return;
            }
            final Object[] elementData = FixedList.this.elementData;
            while (i != size) {
                consumer.accept((E) elementData[i++]);
            }
            // update once at end of iteration to reduce heap write traffic
            cursor = i;
        }
    }
}
