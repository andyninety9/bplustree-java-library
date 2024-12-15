# BPlusTree Library

This is a generic B+ Tree implementation in Java. The library supports **any data type** using **Generics**, and offers **multi-threaded construction** to improve efficiency when inserting large datasets. It is designed for educational purposes and can be extended easily for use in real-world projects.

---

## 📹 **Demonstration Video**
Watch the demonstration for organizing B+-tree on Hadoo on YouTube:  
[![YouTube Video](https://img.youtube.com/vi/xee-5-N1tuA/0.jpg)](https://www.youtube.com/watch?v=xee-5-N1tuA)  
[Click here to watch](https://www.youtube.com/watch?v=xee-5-N1tuA)

## 📋 **Features**
- **Generic Support**: Store any type of data using Generics.
- **Multi-threaded Leaf Node Construction**: Build leaf nodes in parallel.
- **Recursive Internal Node Construction**: Dynamically build the internal levels.
- **Type-safe API**: Ensures compile-time type safety for better error handling.
- **Simple and Intuitive API**: Easy to use and extend.

---

## 🚀 **Prerequisites**
- **Java 17** or higher
- **Gradle 8.x**

---

## 🛠 **Installation**

### **Clone the Repository**
```bash
git clone https://github.com/andyninety9/bptree-library.git
cd bptree-library
```

### **Build the Project Using Gradle**
```bash
./gradlew build
```

### **Run Unit Tests**
```bash
./gradlew test
```

## 💡 **Usage Example**
```bash
import org.bptree.BPlusTree;
import org.bptree.Node;

import java.util.List;

public class Main {
    public static void main(String[] args) throws Exception {
        // Create a B+ Tree with order 4 (each node can hold up to 3 keys)
        BPlusTree<Integer> tree = new BPlusTree<>(4);

        // Insert a list of integers into the tree
        tree.bottom_up_method(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9));

        // Print the height of the tree
        System.out.println("Tree Height: " + tree.getHeight());

        // Access the root node and print its keys
        System.out.println("Root Keys: " + tree.getRoot().getKeys());

        // Traverse the leaf nodes and print their keys
        Node<Integer> current = tree.getRoot();
        while (!current.isLeaf()) {
            current = current.getChildren().get(0);  // Navigate to the first leaf node
        }

        System.out.println("Leaf Node Keys:");
        while (current != null) {
            System.out.println(current.getKeys());  // Print keys in each leaf node
            current = current.getNext();  // Move to the next leaf node
        }
    }
}
```
### **Expected Output**
```bash
Tree Height: 3
Root Keys: [4, 7]
Leaf Node Keys:
[1, 2, 3]
[4, 5, 6]
[7, 8, 9]
```

## 📦 **Project Structure**
```bash
bptree-library/
├── lib/
│   ├── src/
│   │   ├── main/
│   │   │   └── java/org/bptree/  # Source code
│   │   └── test/
│   │       └── java/org/bptree/  # Unit tests
├── build/  # Build artifacts (ignored in .gitignore)
├── build.gradle.kts  # Gradle build file
├── settings.gradle.kts  # Gradle settings
├── .gitignore  # Git ignore file
└── README.md  # Project documentation
```

## 🧪 **Testing**
```bash
./gradlew test
```

### **Sample Unit Test**
Here is a sample unit test from the project:
```bash
import org.bptree.BPlusTree;
import org.bptree.Node;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class BPlusTreeTest {

    @Test
    public void testTreeHeightWithIntegers() throws Exception {
        BPlusTree<Integer> tree = new BPlusTree<>(4);
        tree.bottom_up_method(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9));
        assertEquals(3, tree.getHeight(), "The tree height should be 3.");
    }

    @Test
    public void testLeafNodeLinks() throws Exception {
        BPlusTree<Integer> tree = new BPlusTree<>(4);
        tree.bottom_up_method(List.of(1, 2, 3, 4, 5, 6));

        Node<Integer> firstLeaf = tree.getRoot();
        while (!firstLeaf.isLeaf()) {
            firstLeaf = firstLeaf.getChildren().get(0);
        }

        assertNotNull(firstLeaf.getNext(), "The first leaf node should have a next link.");
        assertEquals(4, firstLeaf.getNext().getKeys().get(0), 
                     "The next leaf node should contain the key 4.");
    }
}
```

## 📜 **License**
This project is licensed under the MIT License. See the LICENSE file for details.

## 🤝 **Contributing**

Contributions are welcome! Please follow these steps to contribute:

1. **Fork the repository.**
2. **Create a new branch** for your feature or bug fix:
```bash
git checkout -b feature/new-feature
```
3. **Commit your changes:** for your feature or bug fix:
```bash
git commit -m "Add new feature"
```
4. **Push to your branch:**
```bash
git push origin feature/new-feature
```
## 🛠  **Built With**
- **Java 17** – Programming language
- **Gradle 8.x** – Build automation tool
- **JUnit 5** – Testing framework  
